
# -*- coding: utf-8 -*-
from __future__ import annotations
import datetime as dt
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import io

import pandas as pd
from dateutil import tz

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

COL_MAP = {
    "Unit": ["unit", "department", "dept", "section"],
    "Role": ["role", "position"],
    "Task": ["task", "mission", "title", "work", "item"],
    "Week": ["week", "wk"],
    "Status": ["status", "state"],
    "StartDate": ["start", "startdate", "begin", "begindate"],
    "DueDate": ["due", "duedate", "deadline", "targetdate"],
    "RescheduledTo": ["rescheduledto", "reschedule_to", "newdeadline", "new_due", "rescheduled_to"],
    "Owner": ["owner", "assigned", "assignee", "responsible"],
    "Notes": ["notes", "remark", "remarks", "comments", "comment"],
    # Extended fields
    "Priority": ["priority", "prio"],
    "Category": ["category", "cat"],
    "Subcategory": ["subcategory", "subcat"],
    "Complexity": ["complexity"],
    "EffortHours": ["efforthours", "effort_hrs", "effort"],
    "Dependency": ["dependency", "depends_on"],
    "Blocker": ["blocker"],
    "RiskLevel": ["risk", "risklevel"],
    "SLA_TargetDays": ["sla_targetdays", "sla_days", "sla_target"],
    "CreatedOn": ["createdon", "created", "created_date", "created_at"],
    "CompletedOn": ["completedon", "completed", "completed_date", "completed_at"],
    "QA_Status": ["qa_status", "qaresult"],
    "QA_Reviewer": ["qa_reviewer", "qareviewer"],
    "Approval_Status": ["approval_status", "approval"],
    "Approval_By": ["approval_by", "approved_by"],
    "KPI_Impact": ["kpi_impact"],
    "KPI_Name": ["kpi_name"],
    "Budget_SAR": ["budget_sar", "budget"],
    "ActualCost_SAR": ["actualcost_sar", "actual_cost", "actuals"],
    "Benefit_Score": ["benefit_score"],
    "Benefit_Notes": ["benefit_notes"],
    "UAT_Date": ["uat_date"],
    "Release_ID": ["release_id"],
    "Change_Request_ID": ["change_request_id", "cr_id", "change_id"],
    "Tags": ["tags"],
}

REQUIRED_MIN = ["Task", "DueDate"]


def to_date(x):
    if pd.isna(x) or x is None or str(x).strip() == "":
        return None
    d = pd.to_datetime(x, errors="coerce")
    if pd.isna(d):
        return None
    return d.date()


def canon_status(s: Optional[str]) -> Optional[str]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    t = str(s).strip().lower()
    return CANON_STATUS.get(t, s if isinstance(s, str) else None)


def normalize_columns(df: pd.DataFrame):
    lower = {c.lower().strip(): c for c in df.columns}
    rename: Dict[str, str] = {}
    missing = []

    for std, alts in COL_MAP.items():
        found = None
        for a in alts:
            if a in lower:
                found = lower[a]
                break
        if found is None and std.lower() in lower:
            found = lower[std.lower()]
        if found:
            rename[found] = std
        else:
            df[std] = None
            if std in REQUIRED_MIN:
                missing.append(std)

    df = df.rename(columns=rename)
    return df, missing


def compute_status_row(row, today: dt.date, due_soon_days: int) -> str:
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
        if 0 <= days_left <= due_soon_days and (current or "").lower() != "done":
            return "Due Soon"
    if current and current not in ["Rescheduled"]:
        return current
    if start and start <= today:
        return "Under Progress"
    return "Not Done"


def process_excel(
    file_bytes: bytes,
    sheet_name: str = "Tasks_Input",
    due_soon_days: int = 3,
    locale_tz: str = "Asia/Riyadh",
    today_override: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame], bytes, Dict[str, str]]:
    """Process the uploaded Excel and return (clean_df, pivots, output_excel_bytes, metadata)."""
    # Load workbook via pandas
    xls = pd.ExcelFile(file_bytes)
    sheets = xls.sheet_names

    # Today (tz aware)
    if today_override:
        today = dt.date.fromisoformat(today_override)
    else:
        tzinfo = tz.gettz(locale_tz)
        today = dt.datetime.now(tzinfo).date()

    # Main data
    if sheet_name not in sheets:
        # fallback to first sheet
        main_df = pd.read_excel(xls, sheet_name=sheets[0])
        sheet_used = sheets[0]
    else:
        main_df = pd.read_excel(xls, sheet_name=sheet_name)
        sheet_used = sheet_name

    df, missing_required = normalize_columns(main_df)

    # Dates
    for col in ["StartDate", "DueDate", "RescheduledTo", "CreatedOn", "CompletedOn", "UAT_Date"]:
        if col in df.columns:
            df[col] = df[col].apply(to_date)

    # Canon status, keep original
    df["Status_Orig"] = df["Status"]
    df["Status"] = df["Status"].apply(canon_status)
    df["DueDate_Orig"] = df["DueDate"]

    # Change log
    change_log = None
    if "Change_Log" in sheets:
        change_log = pd.read_excel(xls, sheet_name="Change_Log")
        change_log.columns = [str(c).strip() for c in change_log.columns]

    approved_ids = set()
    approver_by_id = {}
    if change_log is not None and not change_log.empty:
        if "Change_ID" in change_log.columns and "Status" in change_log.columns:
            for _, row in change_log.iterrows():
                cid = row.get("Change_ID")
                st = str(row.get("Status")).strip().lower() if row.get("Status") is not None else ""
                if cid not in [None, ""] and st == "approved":
                    approved_ids.add(str(cid))
                    approver_by_id[str(cid)] = row.get("Approved_By")

    def apply_approval(row):
        cr_id = row.get("Change_Request_ID")
        if pd.isna(cr_id) or cr_id in [None, ""]:
            return row
        key = str(cr_id).strip()
        if key in approved_ids:
            row["Approval_Status"] = "Approved"
            if not pd.isna(row.get("Approval_By")) and row.get("Approval_By"):
                pass
            else:
                row["Approval_By"] = approver_by_id.get(key, row.get("Approval_By"))
            res = row.get("RescheduledTo")
            if res and (row.get("DueDate") is None or res > row.get("DueDate")):
                row["DueDate"] = res
        return row

    if approved_ids:
        df = df.apply(apply_approval, axis=1)

    # SLA policies
    sla_map = {}
    if "SLA_Policies" in sheets:
        sla = pd.read_excel(xls, sheet_name="SLA_Policies")
        sla.columns = [str(c).strip() for c in sla.columns]
        if all(c in sla.columns for c in ["Category", "Priority", "TargetDays"]):
            for _, r in sla.iterrows():
                key = (str(r["Category"]).strip(), str(r["Priority"]).strip())
                sla_map[key] = r["TargetDays"]

    if sla_map:
        def fill_sla_days(row):
            if pd.isna(row.get("SLA_TargetDays")) or row.get("SLA_TargetDays") in [None, "", 0]:
                cat = str(row.get("Category")).strip() if row.get("Category") is not None else ""
                pri = str(row.get("Priority")).strip() if row.get("Priority") is not None else ""
                return sla_map.get((cat, pri), row.get("SLA_TargetDays"))
            return row.get("SLA_TargetDays")
        df["SLA_TargetDays"] = df.apply(fill_sla_days, axis=1)

    def compute_sla_due(row):
        created = row.get("CreatedOn")
        days = row.get("SLA_TargetDays")
        if created and not pd.isna(days) and str(days) != "":
            try:
                d = int(days)
                return created + dt.timedelta(days=d)
            except Exception:
                return None
        return None

    df["SLA_DueDate"] = df.apply(compute_sla_due, axis=1)

    def compute_sla_breach(row):
        sla_due = row.get("SLA_DueDate")
        if not sla_due:
            return False
        comp = row.get("CompletedOn")
        status = canon_status(row.get("Status"))
        if comp:
            return comp > sla_due
        return dt.date.today() > sla_due and status != "Done"

    df["SLA_Breach"] = df.apply(compute_sla_breach, axis=1)

    # Final status
    df["Status_Final"] = df.apply(lambda r: compute_status_row(r, today, due_soon_days), axis=1)
    df["Status_Final"] = pd.Categorical(df["Status_Final"], categories=STATUS_ORDER, ordered=True)

    # Flags and durations
    df["Is_Overdue"] = df["Status_Final"].eq("Overdue")
    df["Is_DueSoon"] = df["Status_Final"].eq("Due Soon")
    df["Is_Done"] = df["Status_Final"].eq("Done")

    def span_days(d1, d2):
        if d1 and d2:
            return (d2 - d1).days
        return None

    df["Planned_Days"] = df.apply(lambda r: span_days(r.get("StartDate"), r.get("DueDate")), axis=1)
    df["Actual_Days"] = df.apply(lambda r: span_days(r.get("CreatedOn"), r.get("CompletedOn")), axis=1)

    # Pivots
    status_pivot = (
        df.pivot_table(index="Status_Final", values="Task", aggfunc="count", fill_value=0)
          .rename(columns={"Task": "Count"})
          .reset_index()
          .sort_values("Status_Final")
    )
    unit_pivot = (
        df.pivot_table(index="Unit", columns="Status_Final", values="Task", aggfunc="count", fill_value=0)
          .reset_index()
          .sort_values("Unit", na_position="last")
    )
    week_pivot = (
        df.pivot_table(index="Week", columns="Status_Final", values="Task", aggfunc="count", fill_value=0)
          .reset_index()
          .sort_values("Week", na_position="last")
    )
    priority_pivot = (
        df.pivot_table(index="Priority", columns="Status_Final", values="Task", aggfunc="count", fill_value=0)
          .reset_index()
          .sort_values("Priority", na_position="last")
    )
    sla_pivot = (
        df.assign(SLA_State=df["SLA_Breach"].map({True: "Breach", False: "OK"}))
          .pivot_table(index="SLA_State", values="Task", aggfunc="count", fill_value=0)
          .rename(columns={"Task": "Count"})
          .reset_index()
    )
    owner_pivot = (
        df.pivot_table(index="Owner", columns="Status_Final", values="Task", aggfunc="count", fill_value=0)
          .reset_index()
          .sort_values("Owner", na_position="last")
    )

    pivots = {
        "status": status_pivot,
        "unit": unit_pivot,
        "week": week_pivot,
        "priority": priority_pivot,
        "sla": sla_pivot,
        "owner": owner_pivot,
    }

    # Write output Excel to memory
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        df.sort_values([c for c in ["Unit","Priority","Week","Status_Final","DueDate"] if c in df.columns]).to_excel(
            xl, index=False, sheet_name="1_Tasks_Clean"
        )
        pd.DataFrame([
            ["Total Tasks", len(df)],
            ["Done %", round(float(df["Is_Done"].mean() * 100) if len(df) else 0.0, 1)],
            ["Overdue", int(df["Is_Overdue"].sum())],
            ["Due Soon", int(df["Is_DueSoon"].sum())],
            ["SLA Breach", int(df["SLA_Breach"].sum())],
        ], columns=["Metric", "Value"]).to_excel(xl, index=False, sheet_name="2_Weekly_Summary")
        pivots["status"].to_excel(xl, index=False, sheet_name="3_Status_Pivot")
        pivots["unit"].to_excel(xl, index=False, sheet_name="4_Unit_Pivot")
        pivots["week"].to_excel(xl, index=False, sheet_name="5_Week_Pivot")
        pivots["priority"].to_excel(xl, index=False, sheet_name="6_Priority_Pivot")
        pivots["sla"].to_excel(xl, index=False, sheet_name="7_SLA_Pivot")
        pivots["owner"].to_excel(xl, index=False, sheet_name="8_Owner_Pivot")
    buf.seek(0)

    meta = {"sheet_used": sheet_used, "today": today.isoformat()}
    return df, pivots, buf.read(), meta
