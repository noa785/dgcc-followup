import streamlit as st
import pandas as pd
import io
import datetime as dt
from dateutil import tz
from pathlib import Path
import shutil

# your existing DB helpers
from db_core import init_db, get_conn, get_db_path

# ---------------- Page ----------------
st.set_page_config(page_title="Follow-up Manager", layout="wide")
st.title("Follow-up Manager")

# ---------- Canonical status mapping (simple) ----------
SIMPLE_TO_CANON = {
    "Not started": "Not Done",
    "In progress": "Under Progress",
    "Blocked": "Blocked",
    "Rescheduled": "Rescheduled",
    "Done": "Done",
}

STATUS_CHOICES = list(SIMPLE_TO_CANON.keys())

def canon_status(label: str) -> str:
    return SIMPLE_TO_CANON.get(label, "Not Done")

# ---------- Utilities ----------
def to_date(x):
    if x in [None, "", "None"]:
        return None
    try:
        d = pd.to_datetime(x, errors="coerce")
        if pd.isna(d):
            return None
        return d.date()
    except Exception:
        return None

def compute_week(d):
    try:
        return int(pd.to_datetime(d).isocalendar().week)
    except Exception:
        return None

# -------- Auto-backup --------
def _ensure_backup_dir() -> Path:
    p = Path("backups")
    p.mkdir(exist_ok=True)
    return p

def auto_backup_now():
    db_path = get_db_path()
    if db_path.exists():
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = _ensure_backup_dir() / f"followup_{ts}.db"
        shutil.copy2(db_path, dst)

# ---------- DB I/O ----------
def load_tasks() -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql_query("SELECT * FROM tasks", conn)
    finally:
        conn.close()

def insert_task(row: dict):
    cols = list(row.keys())
    vals = [row[c] for c in cols]
    q = f"INSERT INTO tasks ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
    conn = get_conn()
    try:
        conn.execute(q, vals)
        conn.commit()
    finally:
        conn.close()

# ---------- Sidebar ----------
with st.sidebar:
    st.header("⚙️ Options")
    tz_name = st.text_input("Timezone", value=st.session_state.get("tz", "Asia/Riyadh"))
    st.session_state["tz"] = tz_name
    st.caption("Saves to SQLite and auto-backs up after every change.")

# ---------- First run ----------
init_db()

# =========================================================
#           SIMPLE: Add Deliverable + up to 5 Tasks
# =========================================================
st.subheader("Add Deliverable & Tasks")

with st.form("deliverable_form", clear_on_submit=True):
    c1, c2, c3 = st.columns([1.1, 1.2, 0.7])
    Unit = c1.text_input("Unit*", placeholder="e.g., ODU")
    Deliverable = c2.text_input("Deliverable / Project*", placeholder="e.g., Monthly Dashboard")
    Deliv_Due = c3.date_input("Deliverable Due (optional)", value=None, format="YYYY-MM-DD")

    c4, c5 = st.columns([1, 1])
    Deliv_Owner = c4.text_input("Deliverable Owner (optional)")
    Deliv_Notes = c5.text_input("Notes (optional)")

    st.markdown("**Tasks (add 1–5; leave blank rows empty):**")
    task_rows = []
    for i in range(1, 6):
        r1, r2, r3, r4 = st.columns([2.2, 1.2, 1.2, 1.1])
        t_title = r1.text_input(f"Task {i} title", placeholder="e.g., Collect inputs")
        t_status = r2.selectbox(f"Status {i}", STATUS_CHOICES, index=0, key=f"st_{i}")
        t_owner  = r3.text_input(f"Owner {i}", placeholder="(optional)")
        t_due    = r4.date_input(f"Due {i}", value=None, format="YYYY-MM-DD", key=f"due_{i}")
        task_rows.append((t_title, t_status, t_owner, t_due))

    submitted = st.form_submit_button("➕ Save deliverable & tasks")

    if submitted:
        if not Unit or not Deliverable:
            st.error("Please fill **Unit** and **Deliverable / Project**.")
        else:
            added = 0
            for (t_title, t_status, t_owner, t_due) in task_rows:
                if not t_title.strip():
                    continue  # skip blank row

                due_str = str(t_due) if t_due else (str(Deliv_Due) if Deliv_Due else None)
                week = compute_week(due_str) if due_str else None

                row = dict(
                    Unit=Unit.strip(),
                    Role=None,
                    Task=t_title.strip(),
                    Week=week,
                    Status=canon_status(t_status),
                    StartDate=None,
                    DueDate=due_str,
                    RescheduledTo=None,
                    Owner=(t_owner or Deliv_Owner or None),
                    Notes=Deliv_Notes or None,
                    Priority=None,
                    Category=None,
                    Subcategory=Deliverable.strip(),  # GROUP BY deliverable
                    Complexity=None, EffortHours=None, Dependency=None, Blocker=None, RiskLevel=None,
                    SLA_TargetDays=None,
                    CreatedOn=str(dt.date.today()),
                    CompletedOn=None,
                    QA_Status=None, QA_Reviewer=None, Approval_Status=None, Approval_By=None,
                    KPI_Impact=None, KPI_Name=None, Budget_SAR=None, ActualCost_SAR=None,
                    Benefit_Score=None, Benefit_Notes=None, UAT_Date=None, Release_ID=None,
                    Change_Request_ID=None, Tags=None,
                )
                insert_task(row)
                added += 1

            if added:
                auto_backup_now()
                st.success(f"✅ Saved **{added} task(s)** under deliverable **{Deliverable}**.")
            else:
                st.info("No tasks entered. Nothing saved.")

st.divider()

# =========================================================
#           Deliverables Overview (grouped & compact)
# =========================================================
st.subheader("Deliverables Overview")

df = load_tasks()
if df.empty:
    st.info("No data yet. Add your first deliverable above.")
else:
    # normalize useful cols
    df["DueDate"] = pd.to_datetime(df["DueDate"], errors="coerce")
    df["Subcategory"] = df["Subcategory"].fillna("")  # Deliverable
    df["Unit"] = df["Unit"].fillna("")
    today = pd.Timestamp(dt.datetime.now(tz.gettz(st.session_state["tz"]))).normalize()

    # quick flags
    done = df["Status"].eq("Done")
    overdue = (~done) & df["DueDate"].notna() & (df["DueDate"] < today)
    due_soon = (~done) & df["DueDate"].notna() & (df["DueDate"] >= today) & ((df["DueDate"] - today).dt.days <= 3)

    grp = (df.assign(IsDone=done, IsOverdue=overdue, IsSoon=due_soon)
             .groupby(["Unit", "Subcategory"], dropna=False)
             .agg(Total=("Task","count"),
                  Done=("IsDone","sum"),
                  Overdue=("IsOverdue","sum"),
                  DueSoon=("IsSoon","sum"),
                  DueMin=("DueDate","min"),
                  DueMax=("DueDate","max"))
             .reset_index())

    # display compact grid (horizontally)
    for unit, df_u in grp.groupby("Unit"):
        st.markdown(f"### Unit: {unit or '—'}")
        cols = st.columns(3)
        k = 0
        for _, row in df_u.sort_values(["DueMin","Subcategory"]).iterrows():
            with cols[k % 3]:
                st.markdown(
                    f"""<div style="padding:14px;border:1px solid #e5e7eb;border-radius:12px;background:#fff;">
                        <div style="font-weight:600;font-size:18px;margin-bottom:6px;">{row['Subcategory'] or 'Unnamed deliverable'}</div>
                        <div style="color:#6b7280;margin-bottom:6px;">
                          Total: {int(row['Total'])} • Done: {int(row['Done'])} •
                          Due soon: {int(row['DueSoon'])} • Overdue: {int(row['Overdue'])}
                        </div>
                        <div style="color:#6b7280;">Due range: {'' if pd.isna(row['DueMin']) else str(row['DueMin'].date())}
                        {" — " if not pd.isna(row['DueMax']) and not pd.isna(row['DueMin']) else ""}
                        {'' if pd.isna(row['DueMax']) else str(row['DueMax'].date())}</div>
                    </div>""",
                    unsafe_allow_html=True
                )
            k += 1

st.divider()

# =========================================================
#                  All Tasks (simple table)
# =========================================================
st.subheader("All Tasks")
if df.empty:
    st.info("No tasks yet.")
else:
    show = df.copy()
    show = show[["id","Unit","Subcategory","Task","Status","Owner","DueDate","Week","Notes"]].sort_values(["Unit","Subcategory","DueDate","Task"])
    # pretty date
    show["DueDate"] = show["DueDate"].dt.date
    st.dataframe(show, use_container_width=True, hide_index=True)

# =========================================================
#                  Quick export (optional)
# =========================================================
st.download_button(
    "⬇️ Download current tasks (CSV)",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="followup_tasks_export.csv",
    mime="text/csv",
)
