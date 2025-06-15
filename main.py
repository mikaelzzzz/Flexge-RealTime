# fastapi_flexge_notion_sync/main.py
"""
FastAPI service – Flexge ⟶ Notion + limpeza semanal
[… docstring idêntico …]
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

# ---------------------------------------------------------------------------
# Environment ----------------------------------------------------------------
# ---------------------------------------------------------------------------
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DB_ID   = os.getenv("NOTION_DB_ID", "15f206acf37d8068b114db042dd45191")
FLEXGE_API_KEY = os.getenv("FLEXGE_API_KEY")
# ⚠️  agora termina só em /external
FLEXGE_BASE    = os.getenv("FLEXGE_API_BASE", "https://partner-api.flexge.com/external")

if not all([NOTION_API_KEY, FLEXGE_API_KEY]):
    raise RuntimeError("NOTION_API_KEY e FLEXGE_API_KEY precisam estar definidos")

# ---------------------------------------------------------------------------
# Clients --------------------------------------------------------------------
# ---------------------------------------------------------------------------
notion       = NotionClient(auth=NOTION_API_KEY)
httpx_client = httpx.AsyncClient(
    base_url=FLEXGE_BASE,
    headers={"x-api-key": FLEXGE_API_KEY, "accept": "application/json"},
)

# ---------------------------------------------------------------------------
# Helpers (normalize, week_range_iso, map_level, hms) ── inalterados
# ---------------------------------------------------------------------------

# … código helper igual …

# ---------------------------------------------------------------------------
# Duplicate tracking (warm_seen_keys) ── inalterado
# ---------------------------------------------------------------------------

# … idem …

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
        # rota explícita
        r = await httpx_client.get("/students", params=params)
        r.raise_for_status()
        docs = r.json().get("docs", [])
        if not docs:
            break
        students.extend(docs)
        page += 1
    logging.info("Flexge retornou %d alunos", len(students))
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
# Notion helpers + Jobs ── inalterados
# ---------------------------------------------------------------------------

# … idem …

# ---------------------------------------------------------------------------
# FastAPI --------------------------------------------------------------------
# ---------------------------------------------------------------------------
app       = FastAPI(title="Flexge → Notion", version="0.3.1")
scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    await warm_seen_keys()

    # ⏱️  agora a cada 10 min
    scheduler.add_job(sync_job, IntervalTrigger(minutes=10), id="sync")
    scheduler.add_job(
        clean_job,
        CronTrigger(day_of_week="mon", hour=2, minute=0, timezone="UTC"),
        id="weekly-clean",
    )
    scheduler.start()
    logging.info("Scheduler ativo (sync a cada 10 min; limpeza segundas 02:00 UTC)")

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
