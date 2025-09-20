
import streamlit as st
import pandas as pd
from io import BytesIO
from followup_core import process_excel

st.set_page_config(page_title="Follow-up Automation", layout="wide")

st.title("📊 Follow-up Automation — Web App (Prototype)")
st.caption("Upload your Excel (template recommended), I’ll process tasks, approvals, SLA, and generate dashboards + a downloadable output workbook.")

with st.sidebar:
    st.header("⚙️ Options")
    due_soon_days = st.number_input("Due soon window (days)", min_value=1, max_value=14, value=3, step=1)
    sheet_name = st.text_input("Sheet name (leave blank for auto)", value="Tasks_Input")
    today_override = st.text_input("Override 'today' (YYYY-MM-DD)", value="")
    tz_name = st.text_input("Timezone", value="Asia/Riyadh")

uploaded = st.file_uploader("Upload Excel file", type=["xlsx","xlsm"], help="Use the FollowUp_Master_Template.xlsx for best results")

if uploaded is not None:
    try:
        df_clean, pivots, out_bytes, meta = process_excel(
            uploaded.read(),
            sheet_name=sheet_name.strip() or "Tasks_Input",
            due_soon_days=due_soon_days,
            locale_tz=tz_name or "Asia/Riyadh",
            today_override=today_override.strip() or None,
        )
        st.success(f"Processed sheet: {meta['sheet_used']} • As of: {meta['today']}")

        # KPIs
        c1, c2, c3, c4, c5 = st.columns(5)
        total_tasks = len(df_clean)
        done_pct = float(df_clean['Is_Done'].mean() * 100) if total_tasks else 0.0
        c1.metric("Total Tasks", total_tasks)
        c2.metric("Done %", f"{done_pct:.1f}%")
        c3.metric("Overdue", int(df_clean['Is_Overdue'].sum()))
        c4.metric("Due Soon", int(df_clean['Is_DueSoon'].sum()))
        c5.metric("SLA Breach", int(df_clean['SLA_Breach'].sum()))

        st.subheader("🧾 Clean Tasks")
        st.dataframe(df_clean, use_container_width=True, hide_index=True)

        st.subheader("📈 Pivots")
        t1, t2 = st.tabs(["Status / Priority / SLA", "Unit / Week / Owner"])
        with t1:
            st.write("**Status Pivot**"); st.dataframe(pivots["status"], use_container_width=True, hide_index=True)
            st.write("**Priority Pivot**"); st.dataframe(pivots["priority"], use_container_width=True, hide_index=True)
            st.write("**SLA Pivot**"); st.dataframe(pivots["sla"], use_container_width=True, hide_index=True)
        with t2:
            st.write("**Unit Pivot**"); st.dataframe(pivots["unit"], use_container_width=True, hide_index=True)
            st.write("**Week Pivot**"); st.dataframe(pivots["week"], use_container_width=True, hide_index=True)
            st.write("**Owner Pivot**"); st.dataframe(pivots["owner"], use_container_width=True, hide_index=True)

        st.subheader("⬇️ Download Processed Workbook")
        st.download_button(
            label="Download FollowUp_Output.xlsx",
            data=out_bytes,
            file_name="FollowUp_Output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        st.error(f"Failed to process file: {e}")
else:
    st.info("Upload your Excel to start. Tip: the provided template has Status/Priority/Category dropdowns and extra sheets for SLA and Change Log.")
