import streamlit as st
import pandas as pd
import sqlite3
import io
import datetime as dt
from dateutil import tz
from pathlib import Path
import shutil

from db_core import init_db, get_conn, get_db_path

# ---------------- Page ----------------
st.set_page_config(page_title="Follow-up Manager (DB Mode)", page_icon=None, layout="wide")
st.title("Follow-up Manager")

# ---------- Helpers ----------
CANON_STATUS = {
    "done": "Done",
    "completed": "Done",
    "finished": "Done",
    "under progress": "Under Progress",
    "in progress": "Under Progress",
    "progress": "Under Progress",
    "not done": "Not Done",
    "todo": "Not Done",
    "pending": "Not Done",
    "rescheduled": "Rescheduled",
    "deferred": "Rescheduled",
    "blocked": "Blocked",
    "on hold": "Blocked",
}
STATUS_ORDER = ["Overdue", "Due Soon", "Under Progress", "Not Done", "Rescheduled", "Blocked", "Done"]

def canon_status(s):
    if s is None or str(s).strip() == "":
        return None
    return CANON_STATUS.get(str(s).strip().lower(), s)

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

def compute_status_row(row, today, due_soon_days):
    current = canon_status(row.get("Status"))
    due = to_date(row.get("DueDate"))
    start = to_date(row.get("StartDate"))
    resched = to_date(row.get("RescheduledTo"))
    if (current or "").strip().lower() == "done":
        return "Done"
    if resched and resched >= today and (current or "").lower() != "done":
        return "Rescheduled"
    if due:
        if due < today and (current or "").lower() != "done":
            return "Overdue"
        days_left = (due - today).days
        if 0 <= days_left <= st.session_state.get("due_soon_days", 3) and (current or "").lower() != "done":
            return "Due Soon"
    if current and current not in ["Rescheduled"]:
        return current
    if start and start <= today:
        return "Under Progress"
    return "Not Done"

def load_df():
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM tasks ORDER BY DueDate", conn)
    conn.close()
    return df

def upsert_task(row_dict, task_id=None):
    conn = get_conn()
    cur = conn.cursor()
    cols = [c for c in row_dict.keys()]
    vals = [row_dict[c] for c in cols]
    if task_id is None:
        q = f"INSERT INTO tasks ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
        cur.execute(q, vals)
    else:
        set_clause = ",".join([f"{c}=?" for c in cols])
        q = f"UPDATE tasks SET {set_clause} WHERE id=?"
        cur.execute(q, vals + [task_id])
    conn.commit()
    conn.close()

def delete_task(task_id):
    conn = get_conn()
    conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
    conn.commit()
    conn.close()

def get_sla_map():
    conn = get_conn()
    sla = pd.read_sql_query("SELECT Category, Priority, TargetDays FROM sla_policies", conn)
    conn.close()
    m = {}
    for _, r in sla.iterrows():
        key = (str(r["Category"]).strip(), str(r["Priority"]).strip())
        m[key] = r["TargetDays"]
    return m

def get_approved_map():
    conn = get_conn()
    cl = pd.read_sql_query("SELECT Change_ID, Approved_By, Status FROM change_log", conn)
    conn.close()
    m = {}
    for _, r in cl.iterrows():
        if str(r.get("Status")).strip().lower() == "approved":
            m[str(r["Change_ID"])] = r.get("Approved_By")
    return m

def process_df(df, today, due_soon_days=3):
    df = df.copy()
    # apply approvals
    appr = get_approved_map()
    if "Change_Request_ID" in df.columns and len(appr):
        df["Approval_Status"] = df["Approval_Status"].where(df["Change_Request_ID"].isna(), df["Approval_Status"])
        for i, r in df.iterrows():
            cr = r.get("Change_Request_ID")
            if cr and str(cr) in appr:
                df.at[i, "Approval_Status"] = "Approved"
                if not r.get("Approval_By"):
                    df.at[i, "Approval_By"] = appr[str(cr)]
                res = to_date(r.get("RescheduledTo"))
                due = to_date(r.get("DueDate"))
                if res and (not due or res > due):
                    df.at[i, "DueDate"] = res.isoformat()

    # apply SLA
    sla_map = get_sla_map()
    if len(sla_map):
        def fill_sla(row):
            if pd.isna(row.get("SLA_TargetDays")) or row.get("SLA_TargetDays") in [None, "", 0]:
                cat = str(row.get("Category") or "").strip()
                pri = str(row.get("Priority") or "").strip()
                return sla_map.get((cat, pri), row.get("SLA_TargetDays"))
            return row.get("SLA_TargetDays")
        df["SLA_TargetDays"] = df.apply(fill_sla, axis=1)

    def sla_due(row):
        created = to_date(row.get("CreatedOn"))
        days = row.get("SLA_TargetDays")
        try:
            d = int(days) if days not in [None, ""] else None
        except Exception:
            d = None
        return (created + dt.timedelta(days=d)).isoformat() if created and d is not None else None

    df["SLA_DueDate"] = df.apply(sla_due, axis=1)

    def sla_breach(row):
        due = to_date(row.get("SLA_DueDate"))
        if not due:
            return False
        comp = to_date(row.get("CompletedOn"))
        status = canon_status(row.get("Status"))
        if comp:
            return comp > due
        return today > due and status != "Done"

    df["SLA_Breach"] = df.apply(sla_breach, axis=1)
    df["Status_Final"] = df.apply(lambda r: compute_status_row(r, today, due_soon_days), axis=1)

    # flags
    df["Is_Overdue"] = df["Status_Final"].eq("Overdue")
    df["Is_DueSoon"] = df["Status_Final"].eq("Due Soon")
    df["Is_Done"] = df["Status_Final"].eq("Done")
    return df

def export_excel(df_processed):
    piv_status = df_processed.pivot_table(index="Status_Final", values="Task", aggfunc="count", fill_value=0).rename(columns={"Task":"Count"}).reset_index()
    piv_unit = df_processed.pivot_table(index="Unit", columns="Status_Final", values="Task", aggfunc="count", fill_value=0).reset_index()
    piv_week = df_processed.pivot_table(index="Week", columns="Status_Final", values="Task", aggfunc="count", fill_value=0).reset_index()
    piv_pri = df_processed.pivot_table(index="Priority", columns="Status_Final", values="Task", aggfunc="count", fill_value=0).reset_index()
    piv_sla = df_processed.assign(SLA_State=df_processed["SLA_Breach"].map({True:"Breach", False:"OK"})).pivot_table(index="SLA_State", values="Task", aggfunc="count", fill_value=0).rename(columns={"Task":"Count"}).reset_index()
    piv_owner = df_processed.pivot_table(index="Owner", columns="Status_Final", values="Task", aggfunc="count", fill_value=0).reset_index()

    kpi = pd.DataFrame([
        ["Total Tasks", len(df_processed)],
        ["Done %", round(float(df_processed["Is_Done"].mean()*100) if len(df_processed) else 0.0, 1)],
        ["Overdue", int(df_processed["Is_Overdue"].sum())],
        ["Due Soon", int(df_processed["Is_DueSoon"].sum())],
        ["SLA Breach", int(df_processed["SLA_Breach"].sum())],
    ], columns=["Metric", "Value"])

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        df_processed.to_excel(xl, index=False, sheet_name="1_Tasks_Clean")
        kpi.to_excel(xl, index=False, sheet_name="2_Weekly_Summary")
        piv_status.to_excel(xl, index=False, sheet_name="3_Status_Pivot")
        piv_unit.to_excel(xl, index=False, sheet_name="4_Unit_Pivot")
        piv_week.to_excel(xl, index=False, sheet_name="5_Week_Pivot")
        piv_pri.to_excel(xl, index=False, sheet_name="6_Priority_Pivot")
        piv_sla.to_excel(xl, index=False, sheet_name="7_SLA_Pivot")
        piv_owner.to_excel(xl, index=False, sheet_name="8_Owner_Pivot")
    buf.seek(0)
    return buf.read()

# -------- Auto-backup --------
def _ensure_backup_dir() -> Path:
    p = Path("backups")
    p.mkdir(exist_ok=True)
    return p

def auto_backup_now():
    """Copy followup.db to backups/followup_YYYYMMDD_HHMMSS.db and write an Excel snapshot."""
    # 1) DB copy
    db_path = get_db_path()
    if db_path.exists():
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = _ensure_backup_dir() / f"followup_{ts}.db"
        shutil.copy2(db_path, dst)

    # 2) Excel export snapshot
    tzinfo = tz.gettz(st.session_state.get("tz", "Asia/Riyadh"))
    today = dt.datetime.now(tzinfo).date()
    df = load_df()
    if not df.empty:
        processed = process_df(df, today, due_soon_days=st.session_state.get("due_soon_days", 3))
        excel_bytes = export_excel(processed)
        (Path(".") / "FollowUp_AutoBackup.xlsx").write_bytes(excel_bytes)

# ---------- Sidebar ----------
with st.sidebar:
    st.header("⚙️ Options")
    st.session_state["due_soon_days"] = st.number_input("Due soon window (days)", min_value=1, max_value=14, value=3, step=1)
    tz_name = st.text_input("Timezone", value=st.session_state.get("tz", "Asia/Riyadh"))
    st.session_state["tz"] = tz_name  # keep in session for processing/export
    st.caption("Backups auto-save to ./backups and an Excel snapshot is saved as FollowUp_AutoBackup.xlsx")

# ---------- First run ----------
init_db()

# ==================  NO TABS VERSION  ==================

# --- Tasks ---
st.header("Tasks")

st.subheader("Add / Edit Task")
with st.form("task_form", clear_on_submit=True):
    c1, c2, c3, c4 = st.columns(4)
    Unit = c1.text_input("Unit")
    Role = c2.text_input("Role")
    Task = c3.text_input("Task*", placeholder="Required")
    Week = c4.number_input("Week", min_value=1, max_value=55, value=38)

    c5, c6, c7, c8 = st.columns(4)
    Status = c5.selectbox("Status", ["", "Not Done", "Under Progress", "Rescheduled", "Blocked", "Done"])
    StartDate = c6.date_input("StartDate", value=None, format="YYYY-MM-DD")
    DueDate = c7.date_input("DueDate*", value=dt.date.today(), format="YYYY-MM-DD")
    RescheduledTo = c8.date_input("RescheduledTo", value=None, format="YYYY-MM-DD")

    c9, c10, c11, c12 = st.columns(4)
    Owner = c9.text_input("Owner")
    Priority = c10.selectbox("Priority", ["", "Critical", "High", "Medium", "Low"])
    Category = c11.selectbox(
        "Category",
        ["", "Data Cleaning", "File Merge/ETL", "Scheduling", "Reporting", "Dashboard",
         "Access/Permissions", "Integration", "Automation", "Bug Fix", "Change Request",
         "Governance", "Documentation", "Training/Workshop"]
    )
    Subcategory = c12.text_input("Subcategory")

    c13, c14, c15, c16 = st.columns(4)
    CreatedOn = c13.date_input("CreatedOn", value=dt.date.today(), format="YYYY-MM-DD")
    CompletedOn = c14.date_input("CompletedOn", value=None, format="YYYY-MM-DD")
    SLA_TargetDays = c15.number_input("SLA Target Days", min_value=0, max_value=365, value=0)
    Change_Request_ID = c16.text_input("Change Request ID")

    Notes = st.text_area("Notes")

    submitted = st.form_submit_button("➕ Add Task")
    if submitted:
        if not Task or not DueDate:
            st.error("Task and DueDate are required.")
        else:
            row = dict(
                Unit=Unit, Role=Role, Task=Task, Week=int(Week), Status=Status or None,
                StartDate=str(StartDate) if StartDate else None,
                DueDate=str(DueDate),
                RescheduledTo=str(RescheduledTo) if RescheduledTo else None,
                Owner=Owner or None, Notes=Notes or None, Priority=Priority or None,
                Category=Category or None, Subcategory=Subcategory or None,
                Complexity=None, EffortHours=None, Dependency=None, Blocker=None, RiskLevel=None,
                SLA_TargetDays=int(SLA_TargetDays) if SLA_TargetDays else None,
                CreatedOn=str(CreatedOn) if CreatedOn else None,
                CompletedOn=str(CompletedOn) if CompletedOn else None,
                QA_Status=None, QA_Reviewer=None, Approval_Status=None, Approval_By=None,
                KPI_Impact=None, KPI_Name=None, Budget_SAR=None, ActualCost_SAR=None,
                Benefit_Score=None, Benefit_Notes=None, UAT_Date=None, Release_ID=None,
                Change_Request_ID=Change_Request_ID or None, Tags=None
            )
            upsert_task(row)
            auto_backup_now()
            st.success("✅ Task added & backup saved.")

st.divider()
st.subheader("All Tasks")
df = load_df()
if df.empty:
    st.info("No tasks yet. Add your first task above.")
else:
    # Simple edit/delete controls
    edited_id = st.number_input("Edit Task ID", min_value=0, value=0, step=1,
                                help="Enter the 'id' to edit; see table below.")
    if edited_id:
        row = df.loc[df["id"] == edited_id]
        if not row.empty:
            r = row.iloc[0].to_dict()
            st.write("Editing:", r["Task"])
            with st.form("edit_form"):
                new_status = st.selectbox(
                    "Status",
                    ["Not Done", "Under Progress", "Rescheduled", "Blocked", "Done"],
                    index=["Not Done", "Under Progress", "Rescheduled", "Blocked", "Done"].index(r.get("Status") or "Not Done")
                )
                new_due = st.date_input("DueDate", value=to_date(r.get("DueDate")) or dt.date.today(), format="YYYY-MM-DD")
                new_owner = st.text_input("Owner", value=r.get("Owner") or "")
                new_notes = st.text_area("Notes", value=r.get("Notes") or "")
                save = st.form_submit_button("💾 Save changes")
                if save:
                    upsert_task({"Status": new_status, "DueDate": str(new_due), "Owner": new_owner, "Notes": new_notes},
                                task_id=edited_id)
                    auto_backup_now()
                    st.success("✅ Saved & backup updated.")
        else:
            st.warning("ID not found.")

    del_id = st.number_input("Delete Task ID", min_value=0, value=0, step=1)
    if del_id:
        if st.button("🗑️ Confirm delete"):
            delete_task(del_id)
            auto_backup_now()
            st.success("✅ Deleted & backup updated.")

    st.dataframe(df, use_container_width=True, hide_index=True)

# --- Change Log ---
st.header("Change Log")
with st.form("cr_form", clear_on_submit=True):
    c1, c2, c3, c4 = st.columns(4)
    Change_ID = c1.text_input("Change ID*", placeholder="e.g., CR-101")
    Date = c2.date_input("Date", value=dt.date.today(), format="YYYY-MM-DD")
    Requested_By = c3.text_input("Requested By")
    Status = c4.selectbox("Status", ["Submitted", "In Review", "Approved", "Rejected"])
    Description = st.text_area("Description")
    Impact = st.text_input("Impact")
    Approved_By = st.text_input("Approved By")
    Linked_Task = st.number_input("Linked Task ID", min_value=0, value=0, help="Optional")
    submit = st.form_submit_button("➕ Add/Update")
    if submit:
        if not Change_ID:
            st.error("Change ID is required.")
        else:
            conn = get_conn()
            conn.execute(
                """INSERT INTO change_log(Change_ID,Date,Requested_By,Description,Impact,Approved_By,Status,Linked_Task)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(Change_ID) DO UPDATE SET
                   Date=excluded.Date, Requested_By=excluded.Requested_By, Description=excluded.Description,
                   Impact=excluded.Impact, Approved_By=excluded.Approved_By, Status=excluded.Status, Linked_Task=excluded.Linked_Task
                """,
                (Change_ID, str(Date), Requested_By, Description, Impact, Approved_By, Status,
                 int(Linked_Task) if Linked_Task else None)
            )
            conn.commit()
            conn.close()
            auto_backup_now()
            st.success("✅ Saved & backup updated.")

conn = get_conn()
st.dataframe(pd.read_sql_query("SELECT * FROM change_log", conn), use_container_width=True, hide_index=True)
conn.close()

# --- SLA Policies ---
st.header("SLA Policies")
with st.form("sla_form", clear_on_submit=True):
    c1, c2, c3, c4 = st.columns(4)
    Category = c1.text_input("Category*", placeholder="e.g., Reporting")
    Priority = c2.selectbox("Priority*", ["Critical", "High", "Medium", "Low"])
    TargetDays = c3.number_input("TargetDays*", min_value=1, max_value=365, value=5)
    Notes = c4.text_input("Notes")
    submit = st.form_submit_button("➕ Add Policy")
    if submit:
        if not Category or not Priority:
            st.error("Category and Priority are required.")
        else:
            conn = get_conn()
            conn.execute("INSERT INTO sla_policies(Category,Priority,TargetDays,Notes) VALUES(?,?,?,?)",
                         (Category, Priority, int(TargetDays), Notes))
            conn.commit()
            conn.close()
            auto_backup_now()
            st.success("✅ Policy added & backup updated.")

conn = get_conn()
st.dataframe(pd.read_sql_query("SELECT * FROM sla_policies", conn), use_container_width=True, hide_index=True)
conn.close()

# --- Owners ---
st.header("Owners")
with st.form("owners_form", clear_on_submit=True):
    c1, c2, c3, c4 = st.columns(4)
    Owner = c1.text_input("Owner*")
    Email = c2.text_input("Email")
    Role = c3.text_input("Role")
    Unit = c4.text_input("Unit")
    submit = st.form_submit_button("➕ Add Owner")
    if submit:
        if not Owner:
            st.error("Owner is required.")
        else:
            conn = get_conn()
            conn.execute("INSERT INTO owners(Owner,Email,Role,Unit) VALUES(?,?,?,?)",
                         (Owner, Email, Role, Unit))
            conn.commit()
            conn.close()
            auto_backup_now()
            st.success("✅ Owner added & backup updated.")

conn = get_conn()
st.dataframe(pd.read_sql_query("SELECT * FROM owners", conn), use_container_width=True, hide_index=True)
conn.close()

# --- Dashboard & Export ---
st.header("Dashboard & Export")
tzinfo = tz.gettz(st.session_state.get("tz", "Asia/Riyadh"))
today = dt.datetime.now(tzinfo).date()
df = load_df()
if df.empty:
    st.info("No tasks yet.")
else:
    processed = process_df(df, today, due_soon_days=st.session_state["due_soon_days"])
    c1, c2, c3, c4, c5 = st.columns(5)
    total = len(processed)
    done_pct = float(processed["Is_Done"].mean() * 100) if total else 0.0
    c1.metric("Total Tasks", total)
    c2.metric("Done %", f"{done_pct:.1f}%")
    c3.metric("Overdue", int(processed["Is_Overdue"].sum()))
    c4.metric("Due Soon", int(processed["Is_DueSoon"].sum()))
    c5.metric("SLA Breach", int(processed["SLA_Breach"].sum()))

    st.dataframe(processed, use_container_width=True, hide_index=True)

    out_bytes = export_excel(processed)
    st.download_button(
        "⬇️ Download FollowUp_Output.xlsx",
        data=out_bytes,
        file_name="FollowUp_Output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# --- Import Excel (optional) ---
st.header("Import Excel (optional)")
up = st.file_uploader("Upload an Excel with 'Tasks_Input' sheet", type=["xlsx", "xlsm"])
if up is not None:
    try:
        xls = pd.ExcelFile(up.read())
        if "Tasks_Input" in xls.sheet_names:
            df_in = pd.read_excel(xls, sheet_name="Tasks_Input")
        else:
            df_in = pd.read_excel(xls, sheet_name=xls.sheet_names[0])

        # Only keep known cols that exist
        info = pd.read_sql_query("PRAGMA table_info(tasks)", get_conn())
        known_cols = info["name"].tolist()
        keep_cols = [c for c in df_in.columns if c in known_cols]
        df_in = df_in[keep_cols].copy()

        # insert
        conn = get_conn()
        for _, r in df_in.iterrows():
            cols = [c for c in r.index if not pd.isna(r[c])]
            vals = [str(r[c].date()) if hasattr(r[c], "date") else r[c] for c in cols]
            q = f"INSERT INTO tasks ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
            conn.execute(q, vals)
        conn.commit()
        conn.close()
        auto_backup_now()
        st.success(f"Imported {len(df_in)} rows into tasks & saved backup.")
    except Exception as e:
        st.error(f"Import failed: {e}")
