
import sqlite3
from pathlib import Path

DB_PATH = Path("followup.db")

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Unit TEXT,
    Role TEXT,
    Task TEXT NOT NULL,
    Week INTEGER,
    Status TEXT,
    StartDate TEXT,
    DueDate TEXT NOT NULL,
    RescheduledTo TEXT,
    Owner TEXT,
    Notes TEXT,
    Priority TEXT,
    Category TEXT,
    Subcategory TEXT,
    Complexity TEXT,
    EffortHours REAL,
    Dependency TEXT,
    Blocker TEXT,
    RiskLevel TEXT,
    SLA_TargetDays INTEGER,
    CreatedOn TEXT,
    CompletedOn TEXT,
    QA_Status TEXT,
    QA_Reviewer TEXT,
    Approval_Status TEXT,
    Approval_By TEXT,
    KPI_Impact TEXT,
    KPI_Name TEXT,
    Budget_SAR REAL,
    ActualCost_SAR REAL,
    Benefit_Score REAL,
    Benefit_Notes TEXT,
    UAT_Date TEXT,
    Release_ID TEXT,
    Change_Request_ID TEXT,
    Tags TEXT
);

CREATE TABLE IF NOT EXISTS change_log (
    Change_ID TEXT PRIMARY KEY,
    Date TEXT,
    Requested_By TEXT,
    Description TEXT,
    Impact TEXT,
    Approved_By TEXT,
    Status TEXT,
    Linked_Task INTEGER REFERENCES tasks(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS sla_policies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Category TEXT,
    Priority TEXT,
    TargetDays INTEGER,
    Notes TEXT
);

CREATE TABLE IF NOT EXISTS owners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Owner TEXT,
    Email TEXT,
    Role TEXT,
    Unit TEXT
);
"""

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    return conn

def init_db():
    conn = get_conn()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()

def get_db_path() -> Path:
    """Return the path to the SQLite database file so the app can back it up."""
    return DB_PATH
