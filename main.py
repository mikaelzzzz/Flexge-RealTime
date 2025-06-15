# fastapi_flexge_notion_sync/main.py
"""
FastAPI service – Flexge ⟶ Notion + limpeza semanal
===================================================
* sync_job   : busca alunos na Flexge e insere/atualiza no Notion.
* clean_job  : toda segunda-feira 02:00 UTC arquiva todas as páginas do DB.
* Chaves/IDs : via variáveis de ambiente (.env em local dev).
"""
import os
import logging
import unicodedata
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

# ─────────────────────────────────────────────────────────────────────────────
# Environment
# ─────────────────────────────────────────────────────────────────────────────
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DB_ID   = os.getenv("NOTION_DB_ID", "15f206acf37d8068b114db042dd45191")
FLEXGE_API_KEY = os.getenv("FLEXGE_API_KEY")
FLEXGE_BASE    = os.getenv("FLEXGE_API_BASE", "https://partner-api.flexge.com/external")  # ← só /external

if not all([NOTION_API_KEY, FLEXGE_API_KEY]):
    raise RuntimeError("Defina NOTION_API_KEY e FLEXGE_API_KEY nas variáveis de ambiente")

# ─────────────────────────────────────────────────────────────────────────────
# Clients
# ─────────────────────────────────────────────────────────────────────────────
notion       = NotionClient(auth=NOTION_API_KEY)
httpx_client = httpx.AsyncClient(
    base_url=FLEXGE_BASE,
    headers={"x-api-key": FLEXGE_API_KEY, "accept": "application/json"},
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def normalize(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in text if unicodedata.category(ch) != "Mn").lower().strip()

def week_range_iso() -> Tuple[str, str]:
    today = datetime.now(timezone.utc)
    start = (today - timedelta(days=today.weekday())).replace(hour=0, minute=1, second=0, microsecond=0)
    end   = (start + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")

def map_level(level: str) -> str:
    lvl = level.lower()
    if lvl in {"discovery", "adventures"}:
        return "A1"
    return lvl.upper() if len(lvl) == 2 else level.upper()

def hms(seconds: int) -> str:
    return f"{seconds//3600}h{(seconds%3600)//60}m"

# ─────────────────────────────────────────────────────────────────────────────
# Duplicate tracking
# ─────────────────────────────────────────────────────────────────────────────
Seen = Tuple[str, str]      # (nome normalizado, nivel)
seen_keys: Set[Seen] = set()

async def warm_seen_keys() -> None:
    """Carrega as páginas já existentes no Notion para evitar duplicados."""
    logging.info("Carregando páginas existentes do Notion…")
    cursor = None
    while True:
        resp = notion.databases.query(database_id=NOTION_DB_ID, start_cursor=cursor, page_size=100)
        for page in resp["results"]:
            nome  = page["properties"]["Nome"]["title"][0]["plain_text"]
            nivel = page["properties"].get("Nível", {}).get("multi_select", [{}])[0].get("name", "")
            seen_keys.add((normalize(nome), nivel))
        cursor = resp.get("next_cursor")
        if not cursor:
            break
    logging.info("Assinaturas carregadas: %d", len(seen_keys))

# ─────────────────────────────────────────────────────────────────────────────
# Flexge integration
# ─────────────────────────────────────────────────────────────────────────────
async def fetch_students() -> List[Dict]:
    start, end = week_range_iso()
    alunos, page = [], 1
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
        alunos.extend(docs)
        page += 1
    logging.info("Flexge retornou %d alunos", len(alunos))
    return alunos

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

# ─────────────────────────────────────────────────────────────────────────────
# Notion helpers
# ─────────────────────────────────────────────────────────────────────────────
async def page_exists(nome: str) -> str | None:
    resp = notion.databases.query(database_id=NOTION_DB_ID, filter={"property": "Nome", "title": {"equals": nome}}, page_size=1)
    return resp["results"][0]["id"] if resp["results"] else None

async def create_or_update(nome: str, nivel: str, segundos: int) -> None:
    key = (normalize(nome), nivel)
    if key in seen_keys:
        return
    tempo_fmt = hms(segundos)
    page_id = await page_exists(nome)

    if page_id:
        notion.pages.update(page_id=page_id, properties={"Horas de Estudo": {"rich_text": [{"text": {"content": tempo_fmt}}]}})
        logging.info("Atualizado %s → %s", nome, tempo_fmt)
    else:
        notion.pages.create(
            parent={"database_id": NOTION_DB_ID},
            properties={
                "Nome": {"title": [{"text": {"content": nome}}]},
                "Horas de Estudo": {"rich_text": [{"text": {"content": tempo_fmt}}]},
                "Nível": {"multi_select": [{"name": nivel}]},
            },
        )
        logging.info("Criada página para %s (%s | %s)", nome, nivel, tempo_fmt)
    seen_keys.add(key)

# ─────────────────────────────────────────────────────────────────────────────
# Jobs
# ─────────────────────────────────────────────────────────────────────────────
async def sync_job() -> None:
    try:
        alunos = await fetch_students()
        tasks = [create_or_update(st["name"], map_level(await flexge_level(st["id"])), total_time(st)) for st in alunos]
        await asyncio.gather(*tasks)
    except Exception:
        logging.exception("Sync job failed")

async def clean_job() -> None:
    logging.info("Limpeza semanal – arquivando páginas…")
    cursor = None
    while True:
        resp = notion.databases.query(database_id=NOTION_DB_ID, page_size=100, start_cursor=cursor)
        for page in resp["results"]:
            notion.pages.update(page_id=page["id"], archived=True)
        cursor = resp.get("next_cursor")
        if not cursor:
            break
    seen_keys.clear()
    logging.info("Páginas arquivadas e seen_keys limpo.")

# ─────────────────────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────────────────────
app       = FastAPI(title="Flexge → Notion", version="0.3.1")
scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    await warm_seen_keys()
    scheduler.add_job(sync_job, IntervalTrigger(minutes=10), id="sync")  # ← 10 min
    scheduler.add_job(clean_job, CronTrigger(day_of_week="mon", hour=2, minute=0, timezone="UTC"), id="weekly-clean")
    scheduler.start()
    logging.info("Scheduler ativo (sync 10 min; clean segundas 02:00 UTC)")

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
