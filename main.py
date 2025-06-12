# fastapi_flexge_notion_sync/main.py
"""
FastAPI service – Flexge ⟶ Notion + limpeza semanal
===================================================
* **sync_job**: busca alunos na Flexge a cada 10 min e insere/atualiza no Notion.
* **clean_job**: toda segunda‑feira 02:00 UTC arquiva (deleta) **todas** as páginas do database para começar a semana do zero.
  * depois limpa o set `seen_keys` para evitar falsos duplicados.
* Chaves e IDs via `.env`.

Rodar localmente
----------------
```bash
pip install fastapi uvicorn httpx apscheduler python-dotenv notion-client
uvicorn main:app --reload
```
"""

import os
import logging
import unicodedata
import re
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Tuple

import httpx
from fastapi import BackgroundTasks, FastAPI, status
from fastapi.responses import JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from notion_client import Client as NotionClient

# ---------------------------------------------------------------------------
# Environment ----------------------------------------------------------------
# ---------------------------------------------------------------------------
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DB_ID = os.getenv("NOTION_DB_ID", "15f206acf37d8068b114db042dd45191")
FLEXGE_API_KEY = os.getenv("FLEXGE_API_KEY")
FLEXGE_BASE = os.getenv("FLEXGE_API_BASE", "https://partner-api.flexge.com/external")

if not all([NOTION_API_KEY, FLEXGE_API_KEY]):
    raise RuntimeError("NOTION_API_KEY and FLEXGE_API_KEY must be defined in environment")

# ---------------------------------------------------------------------------
# Clients --------------------------------------------------------------------
# ---------------------------------------------------------------------------
notion = NotionClient(auth=NOTION_API_KEY)
httpx_client = httpx.AsyncClient(base_url=FLEXGE_BASE, headers={"x-api-key": FLEXGE_API_KEY, "accept": "application/json"})

# ---------------------------------------------------------------------------
# Helpers --------------------------------------------------------------------
# ---------------------------------------------------------------------------

def normalize(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in text if unicodedata.category(ch) != "Mn").lower().strip()


def week_range_iso() -> Tuple[str, str]:
    today = datetime.now(timezone.utc)
    start = (today - timedelta(days=today.weekday())).replace(hour=0, minute=1, second=0, microsecond=0)
    end = (start + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


def map_level(level: str) -> str:
    level = level.lower()
    if level in ["discovery", "adventures"]:
        return "A1"
    # Converte para maiúsculo e garante o formato correto (A1, A2, B1, etc)
    if len(level) == 2 and level[0] in ['a', 'b', 'c'] and level[1].isdigit():
        return level.upper()
    return level.upper()


def hms(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{int(h)}h{int(m)}m"

# ---------------------------------------------------------------------------
# Duplicate tracking ---------------------------------------------------------
# ---------------------------------------------------------------------------
Seen = Tuple[str, str]  # (normalized name, level)
seen_keys: Set[Seen] = set()

async def warm_seen_keys() -> None:
    logging.info("Loading existing pages from Notion…")
    cursor = None
    while True:
        resp = notion.databases.query(database_id=NOTION_DB_ID, start_cursor=cursor, page_size=100)
        for page in resp["results"]:
            name = page["properties"]["Nome"]["title"][0]["plain_text"]
            level_prop = page["properties"].get("Nível", {})
            level = level_prop.get("multi_select", [{}])[0].get("name", "") if level_prop else ""
            seen_keys.add((normalize(name), level))
        cursor = resp.get("next_cursor")
        if not cursor:
            break
    logging.info("Loaded %d signatures.", len(seen_keys))

# ---------------------------------------------------------------------------
# Flexge integration ---------------------------------------------------------
# ---------------------------------------------------------------------------
async def fetch_students() -> List[Dict]:
    start, end = week_range_iso()
    students, page = [], 1
    while True:
        params = {
            "page": page,
            "isPlacementTestOnly": "false",
            "studiedTimeRange[from]": start,
            "studiedTimeRange[to]": end,
        }
        r = await httpx_client.get("/students", params=params)
        r.raise_for_status()
        docs = r.json().get("docs", [])
        if not docs:
            break
        students.extend(docs)
        page += 1
    logging.info("Flexge returned %d students", len(students))
    return students


def total_time(stu: Dict) -> int:
    t = stu.get("weekTime", {}).get("studiedTime", 0)
    for ex in stu.get("executions", []):
        t += ex.get("studiedTime", 0)
    return t

async def flexge_level(student_id: str) -> str:
    r = await httpx_client.get(f"/students/{student_id}/overview")
    r.raise_for_status()
    courses = r.json().get("activeCourses", [])
    return courses[0].get("name", "Indefinido") if courses else "Indefinido"

# ---------------------------------------------------------------------------
# Notion helpers -------------------------------------------------------------
# ---------------------------------------------------------------------------
async def page_exists(name: str) -> str | None:
    resp = notion.databases.query(
        database_id=NOTION_DB_ID,
        filter={"property": "Nome", "title": {"equals": name}},
        page_size=1,
    )
    if resp["results"]:
        return resp["results"][0]["id"]
    return None

async def create_or_update(name: str, level: str, seconds: int) -> None:
    key = (normalize(name), level)
    if key in seen_keys:
        return

    formatted = hms(seconds)
    page_id = await page_exists(name)

    if page_id:
        notion.pages.update(page_id=page_id, properties={"Horas de Estudo": {"rich_text": [{"text": {"content": formatted}}]}})
        logging.info("Updated %s → %s", name, formatted)
    else:
        notion.pages.create(
            parent={"database_id": NOTION_DB_ID},
            properties={
                "Nome": {"title": [{"text": {"content": name}}]},
                "Horas de Estudo": {"rich_text": [{"text": {"content": formatted}}]},
                "Nível": {"multi_select": [{"name": level}]},
            },
        )
        logging.info("Created page for %s (%s | %s)", name, level, formatted)
    seen_keys.add(key)

# ---------------------------------------------------------------------------
# Jobs -----------------------------------------------------------------------
# ---------------------------------------------------------------------------
async def sync_job() -> None:
    try:
        students = await fetch_students()
        tasks = []
        for st in students:
            sid, name = st["id"], st["name"]
            seconds = total_time(st)
            level_raw = await flexge_level(sid)
            level = map_level(level_raw)
            tasks.append(create_or_update(name, level, seconds))
        await asyncio.gather(*tasks)
    except Exception:
        logging.exception("Sync job failed")

async def clean_job() -> None:
    logging.info("Weekly clean – archiving all pages in Notion DB …")
    cursor = None
    removed = 0
    while True:
        resp = notion.databases.query(database_id=NOTION_DB_ID, page_size=100, start_cursor=cursor)
        for page in resp["results"]:
            notion.pages.update(page_id=page["id"], archived=True)
            removed += 1
        cursor = resp.get("next_cursor")
        if not cursor:
            break
    seen_keys.clear()
    logging.info("Archived %d pages and cleared seen_keys.", removed)

# ---------------------------------------------------------------------------
# FastAPI --------------------------------------------------------------------
# ---------------------------------------------------------------------------
app = FastAPI(title="Flexge → Notion", version="0.3.0")
scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    await warm_seen_keys()

    scheduler.add_job(sync_job, IntervalTrigger(minutes=10), id="sync")
    scheduler.add_job(clean_job, CronTrigger(day_of_week="mon", hour=2, minute=0, timezone="UTC"), id="weekly-clean")
    scheduler.start()
    logging.info("Scheduler running (sync every 10 min; clean Mondays 02:00 UTC)")

@app.on_event("shutdown")
async def shutdown() -> None:
    await httpx_client.aclose()
    scheduler.shutdown()

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}

@app.post("/sync", status_code=status.HTTP_202_ACCEPTED)
async def manual_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(sync_job)
    return JSONResponse({"detail": "Sync scheduled"}, status_code=status.HTTP_202_ACCEPTED)
