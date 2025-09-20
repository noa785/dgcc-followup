# app.py
import io
import pandas as pd
import streamlit as st
import datetime as dt
from dateutil import tz
from supabase import create_client
from db_core import (
    init_db, insert_deliverable, insert_task, fetch_deliverables,
    fetch_tasks_flat, delete_deliverable, insert_archive, fetch_archives
)

# ----------------------- Config -----------------------
TZ = st.secrets.get("app", {}).get("timezone", "Asia/Riyadh")
SUPA_URL = st.secrets["supabase"]["url"]
SUPA_KEY = st.secrets["supabase"]["anon_key"]
SUPA_BUCKET = st.secrets["supabase"]["bucket"]
supabase = create_client(SUPA_URL, SUPA_KEY)

st.set_page_config(page_title="Follow-up Manager", page_icon="🗂️", layout="wide")
st.title("Follow-up Manager")

# ----------------------- Init DB ----------------------
init_db()

# ----------------------- Helpers ---------------------
STATUSES = ["Not started", "In progress", "Blocked", "Done"]

def upload_to_storage(data: bytes, path: str, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") -> str:
    # upsert=true to overwrite same path
    supabase.storage.from_(SUPA_BUCKET).upload(path, data, {"upsert": "true", "contentType": content_type})
    return supabase.storage.from_(SUPA_BUCKET).get_public_url(path)

def export_all_to_excel() -> bytes:
    """One Excel with two sheets: Deliverables & Tasks (flat)."""
    dels = fetch_deliverables()
    tasks = fetch_tasks_flat()
    df_dels = pd.DataFrame([{
        "id": d["id"], "unit": d["unit"], "deliverable": d["name"], "owner": d["owner"],
        "notes": d["notes"], "due_date": d["due_date"], "created_at": d["created_at"]
    } for d in dels])
    df_tasks = pd.DataFrame(tasks or [])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        (df_dels if not df_dels.empty else pd.DataFrame()).to_excel(xl, index=False, sheet_name="Deliverables")
        (df_tasks if not df_tasks.empty else pd.DataFrame()).to_excel(xl, index=False, sheet_name="Tasks")
    buf.seek(0)
    return buf.read()

def archive_selection(ids:list[int], title:str) -> str:
    """Create an Excel containing chosen deliverables + tasks and upload to Storage."""
    # Build data
    all_d = fetch_deliverables()
    selected = [d for d in all_d if d["id"] in ids]
    rows_d = [{
        "id": d["id"], "unit": d["unit"], "deliverable": d["name"],
        "owner": d["owner"], "notes": d["notes"], "due_date": d["due_date"], "created_at": d["created_at"]
    } for d in selected]
    rows_t = []
    for d in selected:
        for t in (d["tasks"] or []):
            rows_t.append({
                "deliverable_id": d["id"],
                "deliverable": d["name"],
                "unit": d["unit"],
                "task_id": t["id"],
                "title": t["title"],
                "status": t["status"],
                "owner": t["owner"],
                "due_date": t["due_date"],
            })
    # Excel
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        pd.DataFrame(rows_d).to_excel(xl, index=False, sheet_name="Deliverables")
        pd.DataFrame(rows_t).to_excel(xl, index=False, sheet_name="Tasks")
    buf.seek(0)
    # Upload
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"archives/{title}_{ts}.xlsx"
    url = upload_to_storage(buf.getvalue(), path)
    # Record archive row
    insert_archive(title=title, file_url=url, items_count=len(selected))
    return url

# ----------------------- Sidebar ----------------------
with st.sidebar:
    st.header("⚙️ Options")
    due_soon_days = st.number_input("Due soon window (days)", 1, 30, 3)
    st.caption("All data is saved in Postgres. Excel backups go to Supabase Storage.")

# ----------------------- Add Deliverable & Tasks ----------------------
st.subheader("Add Deliverable & Tasks")

with st.form("deliverable_form", clear_on_submit=True):
    c1, c2, c3 = st.columns([1, 2, 1])
    unit = c1.text_input("Unit*", placeholder="e.g., ODU")
    deliverable = c2.text_input("Deliverable / Project*", placeholder="e.g., Monthly Dashboard")
    due_date = c3.date_input("Deliverable Due (optional)", value=None, format="YYYY-MM-DD")
    c4, c5 = st.columns([1, 2])
    d_owner = c4.text_input("Deliverable Owner (optional)")
    d_notes = c5.text_input("Notes (optional)")

    st.markdown("**Tasks (add 1–5; leave blank rows empty):**")
    rows = []
    for i in range(1, 6):
        t1, t2, t3 = st.columns([2, 1, 1])
        title = t1.text_input(f"Task {i} title", key=f"title{i}", placeholder="e.g., Collect inputs")
        status = t2.selectbox(f"Status {i}", STATUSES, index=0, key=f"status{i}")
        due = t3.date_input(f"Due {i}", value=None, key=f"due{i}", format="YYYY-MM-DD")
        o1 = st.text_input(f"Owner {i} (optional)", key=f"owner{i}")
        rows.append({"title": title, "status": status, "owner": o1, "due": due})

    submit = st.form_submit_button("➕ Save deliverable & tasks")
    if submit:
        if not unit or not deliverable:
            st.error("Unit and Deliverable are required.")
        else:
            did = insert_deliverable(
                unit=unit.strip(),
                name=deliverable.strip(),
                owner=(d_owner or None),
                notes=(d_notes or None),
                due_date=str(due_date) if due_date else None
            )
            count = 0
            for r in rows:
                if r["title"].strip():
                    insert_task(
                        deliverable_id=did,
                        title=r["title"].strip(),
                        status=r["status"],
                        owner=(r["owner"] or None),
                        due_date=str(r["due"]) if r["due"] else None
                    )
                    count += 1
            st.success(f"Saved ✅  Deliverable '{deliverable}' with {count} task(s).")

# ----------------------- List & Manage ----------------------
st.subheader("Deliverables")
dels = fetch_deliverables()

# Quick grid: 3 per row (cards)
if not dels:
    st.info("No deliverables yet.")
else:
    # Archive builder
    with st.expander("📦 Create an archive from selected deliverables"):
        selectable = {f"#{d['id']} — {d['unit']} / {d['name']}": d["id"] for d in dels}
        selected = st.multiselect("Choose deliverables (any number)", list(selectable.keys()))
        title = st.text_input("Archive title", value="Batch")
        if st.button("Create archive"):
            if not selected:
                st.warning("Pick at least one deliverable.")
            else:
                ids = [selectable[k] for k in selected]
                url = archive_selection(ids, title)
                st.success(f"Archive created → {url}")

    # Cards
    cols = st.columns(3)
    for i, d in enumerate(dels):
        with cols[i % 3]:
            st.markdown(
                f"""
                <div style="padding:14px;border:1px solid #e5e7eb;border-radius:14px;background:#fff;box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px;">
                  <div style="font-size:18px;font-weight:700;margin-bottom:4px;">{d['unit']} — {d['name']}</div>
                  <div style="color:#6b7280;font-size:13px;margin-bottom:6px;">Owner: {d.get('owner') or '—'} | Due: {d.get('due_date') or '—'}</div>
                  <div style="color:#374151;font-size:14px;">{d.get('notes') or ''}</div>
                  <div style="margin-top:10px;color:#6b7280;font-size:13px;">Tasks: {len(d['tasks'] or [])}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("### All tasks (flat)")
    tasks = fetch_tasks_flat()
    df_tasks = pd.DataFrame(tasks or [])
    if df_tasks.empty:
        st.caption("No tasks yet.")
    else:
        # Small computed flags
        tzinfo = tz.gettz(TZ)
        today = dt.datetime.now(tzinfo).date()
        def due_status(row):
            d = row.get("due_date")
            if not d: return ""
            d = pd.to_datetime(d).date()
            if (row.get("status") or "").lower() == "done": return "Done"
            if d < today: return "Overdue"
            if 0 <= (d - today).days <= st.session_state.get("due_soon_days", 3): return "Due soon"
            return ""
        df_tasks["DueFlag"] = df_tasks.apply(due_status, axis=1)
        st.dataframe(df_tasks, use_container_width=True, hide_index=True)

    st.markdown("---")
    # Backup/export buttons
    exp = export_all_to_excel()
    st.download_button("⬇️ Download full Excel snapshot", exp, file_name="FollowUp_Full.xlsx")
    if st.button("☁️ Save full backup to cloud"):
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        url = upload_to_storage(exp, f"backups/FollowUp_Backup_{ts}.xlsx")
        st.success(f"Backup saved → {url}")

# ----------------------- Archives List ----------------------
st.subheader("Archives")
arch = fetch_archives()
if not arch:
    st.caption("No archives yet.")
else:
    st.dataframe(pd.DataFrame(arch), use_container_width=True, hide_index=True)
