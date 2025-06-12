# fastapi_flexge_notion_sync/main.py
"""
FastAPI application that polls the Flexge API every minute for fresh study‑hour data and synchronises
it to Notion, with double‑duplicate protection.

Key design choices
------------------
* **FastAPI** provides HTTP endpoints plus life‑cycle hooks for background polling.
* **httpx.AsyncClient** is used for non‑blocking requests to Flexge.
* **APScheduler** schedules recurring jobs (one‑minute cadence) without manual threading.
* **Duplicate defence**
  1. An in‑memory `seen_keys` set built at start‑up from a Notion search (⏩ first barrier).
  2. A real‑time check with Notion *right before each insert* (⏩ second barrier).
* Secrets are loaded from environment variables ‑ never hard‑coded.
  Create a `.env` (or set variables in Render / Docker) containing::

      NOTION_API_KEY=...
      FLEXGE_API_KEY=...
      FLEXGE_API_BASE=https://partner-api.flexge.com/external/students

Run locally with::

      uvicorn main:app --reload
"""

import os
import re
import asyncio
import logging
import unicodedata
from datetime import datetime, timezone, timedelta
from typing import Dict, Set, Tuple, List

import httpx
from fastapi import FastAPI, BackgroundTasks, status
from fastapi.responses import JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from notion_client import Client as NotionClient

# ---------------------------------------------------------------------------
# Environment & global clients
# ---------------------------------------------------------------------------
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
FLEXGE_API_BASE = os.getenv("FLEXGE_API_BASE", "https://partner-api.flexge.com/external/students")
FLEXGE_API_KEY = os.getenv("FLEXGE_API_KEY")

if not all([NOTION_API_KEY, FLEXGE_API_KEY]):
    raise RuntimeError("Missing one or more required environment variables: NOTION_API_KEY, FLEXGE_API_KEY")

notion = NotionClient(auth=NOTION_API_KEY)
httpx_client = httpx.AsyncClient(base_url=FLEXGE_API_BASE, headers={"x-api-key": FLEXGE_API_KEY})

# ---------------------------------------------------------------------------
# Configurable IDs
# ---------------------------------------------------------------------------
DB_NOTES_INPUT = os.getenv("NOTION_DB_INPUT", "13e206acf37d8012b5e4c1f1e7e6391e")
DB_REPORTS_REVIEW = os.getenv("NOTION_DB_REVIEW", "14d206ac-f37d-8038-8c24-fb4cd9c6b8e3")
TEACHER_DB_MAP = {
    "Teacher Mayara": "14d206acf37d80a6a49ffd315d2b4b05",
    "Teacher Karina": "159206acf37d80e39c4ef52ded59a1be",
    "Teacher Karol": "159206acf37d8020985adfe948a04f39",
    "Teacher Vanessa": "183206acf37d805d88abc1fdad61e831",
    "Teacher Jhouselyn": "199206acf37d811d83adf0fecf9ecd63",
}

# student_pages_map shortened for brevity – import from separate module or use Notion search if preferred
from student_pages import student_pages_map  # noqa: E402, assume you put the dict in student_pages.py

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_string(s: str) -> str:
    """Remove accents and lowercase for robust comparisons."""
    s = unicodedata.normalize("NFD", s)
    s = s.encode("ascii", "ignore").decode("utf-8")
    return s.lower().strip()


def combine_multi_select(property_dict: Dict) -> str:
    mul = property_dict.get("multi_select", [])
    return " ".join(i["name"].strip() for i in mul if "name" in i)


def extract_level(text: str) -> str:
    valid = {"PREA1", "PRE-A1", "A1", "A2", "B1", "B2", "C1", "C2"}
    for token in re.split(r"[^A-Za-z0-9]+", text.upper()):
        if token in valid:
            return "A1" if token.startswith("PRE") else token
    return "unknown"


def safe_rich_text(prop: Dict, default: str = "") -> str:
    rt = prop.get("rich_text", [])
    if rt and "text" in rt[0]:
        return rt[0]["text"].get("content", "").strip() or default
    return default


# ---------------------------------------------------------------------------
# Duplicate tracking – first barrier (in‑memory)
# ---------------------------------------------------------------------------
SeenKey = Tuple[str, str]  # (student_name, class_date)
seen_keys: Set[SeenKey] = set()


async def warm_seen_keys() -> None:
    """Load existing keys from the review DB into memory on startup."""
    logging.info("Pre‑loading existing report signatures from Notion REVIEW DB ...")
    cursor = None
    while True:
        resp = notion.databases.query(database_id=DB_REPORTS_REVIEW, page_size=100, start_cursor=cursor)
        for page in resp["results"]:
            props = page["properties"]
            try:
                sname = props["Student Name"]["rich_text"][0]["plain_text"]
                cdate = props["Class Date"]["date"]["start"]
                seen_keys.add((normalize_string(sname), cdate))
            except Exception:
                continue
        cursor = resp.get("next_cursor")
        if not cursor:
            break
    logging.info("Loaded %d existing report signatures.", len(seen_keys))


# ---------------------------------------------------------------------------
# Flexge polling logic
# ---------------------------------------------------------------------------
async def fetch_flexge_updates() -> List[Dict]:
    """Call Flexge API and return the list of *new* class reports since last run."""
    try:
        # Calculate date range
        today = datetime.now(timezone.utc)
        start_of_week = (today - timedelta(days=today.weekday())).replace(hour=0, minute=1, second=0, microsecond=0)
        end_of_week = (start_of_week + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
        
        # Format dates for API
        start_date = start_of_week.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_date = end_of_week.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Parameters matching the working Flask version
        params = {
            'page': 1,
            'isPlacementTestOnly': 'false',
            'studiedTimeRange[from]': start_date,
            'studiedTimeRange[to]': end_date,
        }
        
        resp = await httpx_client.get("", params=params)
        resp.raise_for_status()
        data = resp.json()
        students = data.get('docs', [])
        
        # Process each student
        results = []
        for student in students:
            student_id = student.get('id')
            # Get student level
            overview_resp = await httpx_client.get(f"/{student_id}/overview")
            overview_resp.raise_for_status()
            overview_data = overview_resp.json()
            active_courses = overview_data.get('activeCourses', [])
            level = active_courses[0].get('name', 'Indefinido') if active_courses else 'Indefinido'
            if level == "Adventures":
                level = "A1"
            
            # Calculate total study time
            total_time = student.get('weekTime', {}).get('studiedTime', 0)
            for execution in student.get('executions', []):
                total_time += execution.get('studiedTime', 0)
            
            # Format the result
            results.append({
                'student_name': student.get('name'),
                'level': level,
                'total_time': total_time,
                'class_date': start_date  # Using start date as class date
            })
            
        return results
    except httpx.HTTPError as exc:
        logging.error("Flexge API error: %s", exc)
        return []


async def process_flexge_record(rec: Dict) -> None:
    """Process a single Flexge class record and push to Notion after dedup."""
    student_name_raw = rec.get("student_name", "Unnamed Student")
    student_name_norm = normalize_string(student_name_raw)
    class_date = rec.get("class_date")  # ISO 8601 string

    sig: SeenKey = (student_name_norm, class_date)
    if sig in seen_keys:
        logging.info("Skipping duplicate record for %s on %s (memory cache).", student_name_raw, class_date)
        return

    # 🔒 Second barrier – query Notion for the same combination before insert
    dup_filter = {
        "and": [
            {"property": "Student Name", "rich_text": {"equals": student_name_raw}},
            {"property": "Class Date", "date": {"equals": class_date}},
        ]
    }
    dup_resp = notion.databases.query(database_id=DB_REPORTS_REVIEW, page_size=1, filter=dup_filter)
    if dup_resp.get("results"):
        seen_keys.add(sig)  # add to cache to avoid requerying next time
        logging.info("Duplicate detected in Notion – skipping %s on %s.", student_name_raw, class_date)
        return

    # Prepare fields
    theme = rec.get("theme", "No Theme")
    overview = rec.get("overview", "ABSENT STUDENT")
    recommendations = rec.get("recommendations", "ABSENT STUDENT")
    level_string = rec.get("level", theme)  # fallback
    level = extract_level(level_string)

    # Insert into Notion REVIEW DB as Pending Review
    try:
        notion.pages.create(
            parent={"database_id": DB_REPORTS_REVIEW},
            properties={
                "Student Name": {"rich_text": [{"text": {"content": student_name_raw}}]},
                "Class Theme": {"rich_text": [{"text": {"content": theme}}]},
                "Class Date": {"date": {"start": class_date}},
                "Performance Overview": {"rich_text": [{"text": {"content": overview}}]},
                "Recommendations for Enhancement": {"rich_text": [{"text": {"content": recommendations}}]},
                "Which teacher does the student belong to?": {"multi_select": [{"name": rec.get("teacher", "Unknown Teacher")} ]},
                "Status": {"select": {"name": "Pending Review"}},
            },
        )
        seen_keys.add(sig)
        logging.info("Report for %s (%s) inserted to Notion REVIEW.", student_name_raw, class_date)
    except Exception as exc:
        logging.error("Failed to insert Notion page: %s", exc)


async def flexge_job() -> None:
    """Main scheduled job: fetch updates and push to Notion."""
    updates = await fetch_flexge_updates()
    tasks = [process_flexge_record(rec) for rec in updates]
    if tasks:
        await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# FastAPI app & scheduler
# ---------------------------------------------------------------------------
app = FastAPI(title="Flexge → Notion sync", version="0.1.0")

scheduler = AsyncIOScheduler()


@app.on_event("startup")
async def startup_event() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    await warm_seen_keys()

    # schedule flexge polling every 60 seconds
    scheduler.add_job(flexge_job, IntervalTrigger(seconds=60), id="flexge-poll")
    scheduler.start()
    logging.info("Scheduler started – polling Flexge every 60 seconds.")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await httpx_client.aclose()
    scheduler.shutdown()


# Manual trigger endpoints --------------------------------------------------

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.post("/sync", status_code=status.HTTP_202_ACCEPTED)
async def manual_sync(background_tasks: BackgroundTasks):
    """Manually trigger Flexge sync without waiting for scheduler."""
    background_tasks.add_task(flexge_job)
    return JSONResponse(content={"detail": "Sync scheduled."}, status_code=status.HTTP_202_ACCEPTED)
