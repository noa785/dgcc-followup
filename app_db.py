# db_core.py
import sqlite3
import datetime as dt
from pathlib import Path
import json

DB_PATH = Path("followup.db")


# ---------------- Core ----------------
def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create base tables and run additive migrations (safe on existing DB)."""
    conn = _connect()
    cur = conn.cursor()

    # Base tables (first-time)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS deliverables(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unit TEXT,
        name TEXT NOT NULL,
        owner TEXT,
        owner_email TEXT,
        notes TEXT,
        due_date TEXT,
        status TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tasks(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deliverable_id INTEGER,
        task TEXT NOT NULL,
        owner TEXT,
        notes TEXT,
        due_date TEXT,
        status TEXT,
        FOREIGN KEY(deliverable_id) REFERENCES deliverables(id) ON DELETE CASCADE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS archives(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope TEXT NOT NULL,            -- 'deliverable' or 'task'
        payload_json TEXT NOT NULL,
        archived_at TEXT NOT NULL
    );
    """)

    conn.commit()

    # Additive migrations
    def _ensure_cols(table, cols):
        cur.execute(f"PRAGMA table_info({table});")
        have = {r["name"] for r in cur.fetchall()}
        for col, typ in cols:
            if col not in have:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ};")

    # Newer deliverable fields
    _ensure_cols("deliverables", [
        ("priority", "TEXT"),         # Low/Medium/High/Critical
        ("category", "TEXT"),
        ("tags", "TEXT"),
        ("expected_hours", "REAL"),
        ("start_date", "TEXT"),
        ("last_update", "TEXT")
    ])

    # Newer task fields
    _ensure_cols("tasks", [
        ("priority", "TEXT"),
        ("tags", "TEXT"),
        ("expected_hours", "REAL"),
        ("start_date", "TEXT"),
        ("last_update", "TEXT"),
        ("blocked_reason", "TEXT")
    ])

    conn.commit()
    conn.close()


# ---------------- Inserts/Updates ----------------
def insert_deliverable(
    unit, name, owner, owner_email, notes, due_date, status,
    priority=None, category=None, tags=None, expected_hours=None,
    start_date=None
):
    conn = _connect()
    cur = conn.cursor()
    now = dt.datetime.utcnow().isoformat()
    cur.execute("""
        INSERT INTO deliverables(
            unit, name, owner, owner_email, notes, due_date, status,
            priority, category, tags, expected_hours, start_date, last_update
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        unit, name, owner, owner_email, notes, due_date, status,
        priority, category, tags, expected_hours, start_date, now
    ))
    conn.commit()
    did = cur.lastrowid
    conn.close()
    return did


def insert_task(
    deliverable_id, task, owner, notes, due_date, status,
    priority=None, tags=None, expected_hours=None,
    start_date=None, blocked_reason=None
):
    conn = _connect()
    cur = conn.cursor()
    now = dt.datetime.utcnow().isoformat()
    cur.execute("""
        INSERT INTO tasks(
            deliverable_id, task, owner, notes, due_date, status,
            priority, tags, expected_hours, start_date, last_update, blocked_reason
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        deliverable_id, task, owner, notes, due_date, status,
        priority, tags, expected_hours, start_date, now, blocked_reason
    ))
    conn.commit()
    conn.close()


def touch_deliverable_last_update(deliverable_id):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("UPDATE deliverables SET last_update=? WHERE id=?",
                (dt.datetime.utcnow().isoformat(), deliverable_id))
    conn.commit()
    conn.close()


def delete_deliverable(deliverable_id):
    """Archive then delete deliverable + its tasks."""
    conn = _connect()
    cur = conn.cursor()

    cur.execute("SELECT * FROM deliverables WHERE id=?", (deliverable_id,))
    d = cur.fetchone()
    if d:
        archive_payload = dict(d)
        cur.execute("SELECT * FROM tasks WHERE deliverable_id=?", (deliverable_id,))
        trows = [dict(r) for r in cur.fetchall()]
        archive_payload["_tasks"] = trows
        cur.execute("""
            INSERT INTO archives(scope, payload_json, archived_at)
            VALUES (?,?,?)
        """, ("deliverable", json.dumps(archive_payload), dt.datetime.utcnow().isoformat()))

    cur.execute("DELETE FROM tasks WHERE deliverable_id=?", (deliverable_id,))
    cur.execute("DELETE FROM deliverables WHERE id=?", (deliverable_id,))
    conn.commit()
    conn.close()


# ---------------- Fetches ----------------
def fetch_deliverables():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, unit, name, owner, owner_email, notes, due_date, status,
               priority, category, tags, expected_hours, start_date, last_update
        FROM deliverables
        ORDER BY COALESCE(due_date,'9999-12-31') ASC, priority DESC, id ASC;
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def fetch_tasks_for(deliverable_id):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, deliverable_id, task, owner, notes, due_date, status,
               priority, tags, expected_hours, start_date, last_update, blocked_reason
        FROM tasks
        WHERE deliverable_id=?
        ORDER BY COALESCE(due_date,'9999-12-31') ASC, priority DESC, id ASC;
    """, (deliverable_id,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def fetch_tasks_flat():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.id, t.deliverable_id, d.name AS deliverable_name,
               t.task, t.owner, t.notes, t.due_date, t.status,
               t.priority, t.tags, t.expected_hours, t.start_date, t.last_update, t.blocked_reason
        FROM tasks t
        LEFT JOIN deliverables d ON d.id = t.deliverable_id
        ORDER BY COALESCE(t.due_date,'9999-12-31') ASC, t.priority DESC, t.id ASC;
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def fetch_archives(limit=200):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, scope, payload_json, archived_at
        FROM archives
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows
