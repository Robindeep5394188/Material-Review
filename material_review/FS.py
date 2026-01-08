from pathlib import Path

import pandas as pd
import streamlit as st

FS_PROCESSED_PATH = Path(r"P:\Shared\From QC\Fragrance Screening Planning\FS Query.xlsx")
FS_STATUS_PATH = Path(r"\\PCCSTR\dept\QC\Access Share\Fragrance Screening.xlsm")

def add_serial(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.reset_index(drop=True).copy()
    col_names = df2.columns.astype(str)
    df2 = df2.loc[:, ~col_names.str.match(r"^Unnamed:")]
    df2 = df2.loc[:, col_names.str.strip() != ""]
    df2.insert(0, "#", range(1, len(df2) + 1))
    return df2


@st.cache_data(show_spinner=False)
def load_processed_requests(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_excel(path, sheet_name="Processed", engine="openpyxl")


def save_processed_requests(path: Path, df: pd.DataFrame) -> None:
    try:
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, index=False, sheet_name="Processed")
    except Exception as exc:
        st.error(f"Failed to save processed requests: {exc}")

def remove_leading_zeros(value: str) -> str:
    text = (value or "").strip()
    while text.startswith("0") and len(text) > 1:
        text = text[1:]
    return text or "0"


def normalize_key(value: str) -> str:
    return remove_leading_zeros(str(value).strip())


def build_active_frg_lot(
    df_active: pd.DataFrame,
    df_voided: pd.DataFrame,
    df_processed: pd.DataFrame,
) -> pd.DataFrame:
    if df_active is None or df_active.empty:
        return pd.DataFrame(columns=["FRG#", "FRG lot#", "FRG qty"])

    def _col(df: pd.DataFrame, idx: int) -> pd.Series:
        if df is None or df.empty:
            return pd.Series(dtype=str)
        if df.shape[1] <= idx:
            return pd.Series([""] * len(df))
        return df.iloc[:, idx]

    void_a = _col(df_voided, 0).astype(str).map(normalize_key)
    void_b = _col(df_voided, 1).astype(str).map(normalize_key)
    voided_keys = set((void_a + "|" + void_b).tolist())

    proc_a = _col(df_processed, 4).astype(str).map(normalize_key)
    proc_b = _col(df_processed, 5).astype(str).map(normalize_key)
    processed_keys = set((proc_a + "|" + proc_b).tolist())

    out_map: dict[str, float] = {}
    active_a = _col(df_active, 0).astype(str)
    active_b = _col(df_active, 1).astype(str)
    active_c = pd.to_numeric(_col(df_active, 2), errors="coerce").fillna(0)
    for frg, lot, qty in zip(active_a, active_b, active_c):
        key = f"{normalize_key(frg)}|{normalize_key(lot)}"
        if key in voided_keys or key in processed_keys:
            continue
        out_map[key] = out_map.get(key, 0) + float(qty)

    rows = []
    for key, qty in out_map.items():
        frg, lot = key.split("|", 1)
        rows.append({"FRG#": frg, "FRG lot#": lot, "FRG qty": qty})
    return pd.DataFrame(rows)


def save_active_frg_lot(path: Path, df: pd.DataFrame) -> None:
    try:
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, index=False, sheet_name="Active FRG Lot")
    except Exception as exc:
        st.error(f"Failed to save Active FRG Lot: {exc}")

def load_sheet(path: Path, sheet_name: str) -> pd.DataFrame | None:
    try:
        with pd.ExcelFile(path, engine="openpyxl") as excel:
            if sheet_name not in excel.sheet_names:
                return None
        return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    except Exception:
        return None


def get_active_frg_df(path: Path) -> pd.DataFrame | None:
    active_df = st.session_state.get("fs_active_frg_df")
    if isinstance(active_df, pd.DataFrame) and not active_df.empty:
        return active_df.copy()
    return load_sheet(path, "Active FRG")


def _get_priority(status: str) -> int:
    s = (status or "").strip().upper()
    if s.startswith("RELEASED WITH"):
        return 7
    if s == "RE-TEST":
        return 6
    if s == "NOT RELEASED":
        return 5
    if s == "TEST ADDED":
        return 4
    if s == "IN BURN":
        return 3
    if s == "SCHEDULED":
        return 2
    if s == "BATCHING":
        return 1
    return 0


@st.cache_data(show_spinner=False)
def load_external_status(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_excel(path, sheet_name="All", engine="openpyxl", usecols="N,W,Y")


def build_status_map(df_ext: pd.DataFrame) -> dict[str, dict[str, str]]:
    if df_ext is None or df_ext.empty:
        return {}

    df = df_ext.copy()
    df.columns = ["Unique ID", "Status", "Status date"]

    status_map: dict[str, dict[str, str]] = {}
    for _idx, row in df.iterrows():
        uid = str(row.get("Unique ID", "")).strip()
        status = str(row.get("Status", "")).strip()
        status_date = "" if pd.isna(row.get("Status date")) else str(row.get("Status date")).strip()
        if not uid:
            continue
        status_map.setdefault(uid, {"status": "", "status_date": "", "releases": set()})
        entry = status_map[uid]

        if status.upper().startswith("RELEASED WITH"):
            clean_rel = status.replace("Released with", "", 1).strip()
            if clean_rel:
                entry["releases"].add(clean_rel)
            if not entry["status_date"] and status_date:
                entry["status_date"] = status_date
        else:
            if _get_priority(status) > _get_priority(entry["status"]):
                entry["status"] = status
                entry["status_date"] = status_date or entry["status_date"]

    finalized: dict[str, dict[str, str]] = {}
    for uid, entry in status_map.items():
        releases = sorted(entry["releases"])
        if releases:
            status_text = "Released with " + ", ".join(releases)
        elif entry["status"]:
            status_text = "Re-Testing" if entry["status"].strip().upper() == "RE-TEST" else entry["status"]
        else:
            status_text = "In Queue"
        finalized[uid] = {
            "status": status_text,
            "status_date": entry["status_date"],
        }
    return finalized


def apply_status_updates(df_processed: pd.DataFrame, status_map: dict[str, dict[str, str]]) -> pd.DataFrame:
    if df_processed is None or df_processed.empty:
        return df_processed

    df = df_processed.copy()
    if "Unique ID" not in df.columns:
        st.error("Processed sheet missing 'Unique ID' column.")
        return df

    status_vals = []
    date_vals = []
    for uid in df["Unique ID"].astype(str):
        rec = status_map.get(uid.strip())
        if rec:
            status_vals.append(rec.get("status", ""))
            date_vals.append(rec.get("status_date", ""))
        else:
            status_vals.append("In Queue")
            date_vals.append("")

    if "Status" in df.columns:
        df["Status"] = status_vals
    else:
        df.insert(len(df.columns), "Status", status_vals)

    if "Status date" in df.columns:
        df["Status date"] = date_vals
    else:
        df.insert(len(df.columns), "Status date", date_vals)

    return df


def refresh_statuses(df_processed: pd.DataFrame) -> pd.DataFrame:
    external_df = load_external_status(FS_STATUS_PATH)
    status_map = build_status_map(external_df)
    return apply_status_updates(df_processed, status_map)


def clear_fs_search() -> None:
    st.session_state["fs_search"] = ""


def mark_fs_changes() -> None:
    st.session_state["fs_pending_changes"] = True


def render_fs_requests_page() -> None:
    st.markdown(
        """
        <style>
        #fs-header h2 {
          margin: -0.4rem 0 0 0;
          padding: 0;
          line-height: 1.05;
        }
        #fs-header p {
          margin: 0 0 0.1rem 0;
          color: #6b7280;
          font-size: 0.875rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <style>
        #fs-search { position: relative; width: 100%; }
        #fs-search input {
          height: 34px !important;
          padding: 4px 42px 4px 10px !important;
          border-radius: 12px !important;
          font-size: 12px !important;
          max-width: 420px !important;
        }
        #fs-iconbar{
          display:flex;
          align-items:center;
          justify-content:flex-end;
          gap:6px;
          margin-top:-6px;
        }
        #fs-iconbar .stButton > button{
          width: 42px !important;
          height: 42px !important;
          min-width: 42px !important;
          max-width: 42px !important;
          padding: 0 !important;
          border-radius: 12px !important;
          display: inline-flex !important;
          align-items: center !important;
          justify-content: center !important;
          font-size: 18px !important;
          background: #ffffff !important;
          border: 1px solid #e5e7eb !important;
          color: #111827 !important;
          line-height: 1 !important;
        }
        #fs-iconbar .stButton > button:hover{
          border-color:#cbd5e1 !important;
        }

        #fs-toolbar {
          margin-top: 0;
          margin-bottom: -0.6rem;
        }

        #fs-table {
          margin-top: -0.6rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if not FS_PROCESSED_PATH.exists():
        st.error(f"Processed file not found: {FS_PROCESSED_PATH}")
        return

    df_processed = load_processed_requests(FS_PROCESSED_PATH)
    if df_processed.empty:
        st.info("No processed requests loaded yet.")
        df_processed = pd.DataFrame()

    edit_mode = st.session_state.get("fs_edit_mode", False)
    st.session_state.setdefault("fs_pending_changes", False)
    st.session_state.setdefault("fs_confirm_delete", False)

    st.markdown('<div id="fs-toolbar">', unsafe_allow_html=True)
    toolbar = st.container()
    t_title, t_search, _t_clear, t_refresh, t_process, t_edit, t_save, t_filter = toolbar.columns(
        [20, 3.2, 0.55, 1, 1, 1, 1, 1],
        vertical_alignment="center",
    )
    with t_title:
        st.markdown(
            """
            <div id="fs-header">
              <h2>FS Requests</h2>
              <p>Processed requests.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with t_search:
        st.markdown('<div id="fs-search">', unsafe_allow_html=True)
        st.text_input("Search", key="fs_search", placeholder="Search…", label_visibility="collapsed")
        if (st.session_state.get("fs_search") or "").strip():
            st.button("✕", key="btn_clear_fs_search", help="Clear search", on_click=clear_fs_search)
        st.markdown("</div>", unsafe_allow_html=True)
    with t_refresh:
        st.markdown('<div id="fs-iconbar">', unsafe_allow_html=True)
        refresh_clicked = st.button("↻", help="Refresh status from Fragrance Screening.xlsm")
        st.markdown("</div>", unsafe_allow_html=True)
    with t_process:
        st.markdown('<div id="fs-iconbar">', unsafe_allow_html=True)
        process_clicked = st.button("⚙", help="Process new tests")
        st.markdown("</div>", unsafe_allow_html=True)
    with t_edit:
        st.markdown('<div id="fs-iconbar">', unsafe_allow_html=True)
        edit_clicked = st.button("✎", help="Enable editing")
        st.markdown("</div>", unsafe_allow_html=True)
    with t_save:
        save_disabled = not edit_mode or not st.session_state.get("fs_pending_changes", False)
        save_type = "primary" if not save_disabled else "secondary"
        st.markdown('<div id="fs-iconbar">', unsafe_allow_html=True)
        save_clicked = st.button(
            "💾",
            help="Save processed updates",
            disabled=save_disabled,
            type=save_type,
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with t_filter:
        with st.popover("👁", help="Filter Status date"):
            status_date_filter = st.checkbox("Filter by Status date", key="fs_status_date_filter")
            status_date_from = st.date_input(
                "Status date from",
                key="fs_status_date_from",
                value=st.session_state.get("fs_status_date_from") if status_date_filter else None,
            )
            status_date_to = st.date_input(
                "Status date to",
                key="fs_status_date_to",
                value=st.session_state.get("fs_status_date_to") if status_date_filter else None,
            )
    st.markdown("</div>", unsafe_allow_html=True)

    if edit_clicked:
        edit_mode = not edit_mode
        st.session_state["fs_edit_mode"] = edit_mode

    if st.session_state.get("fs_status_auto_loaded") != True:
        df_processed = refresh_statuses(df_processed)
        st.session_state["fs_status_auto_loaded"] = True

    if refresh_clicked:
        df_processed = refresh_statuses(df_processed)
        save_processed_requests(FS_PROCESSED_PATH, df_processed)
        st.cache_data.clear()
    if process_clicked:
        df_active = get_active_frg_df(FS_PROCESSED_PATH)
        df_voided = load_sheet(FS_PROCESSED_PATH, "Voided")
        if df_active is None or df_voided is None:
            st.info("Process new data requires Active FRG data and a Voided sheet.")
            st.stop()
        new_data = build_active_frg_lot(df_active, df_voided, df_processed)
        save_active_frg_lot(FS_PROCESSED_PATH, new_data)
        st.session_state["fs_new_data"] = new_data
        st.success("New data processed.")
    filtered = add_serial(df_processed)
    search = (st.session_state.get("fs_search") or "").strip().lower()
    if search:
        blob = (
            filtered.astype(str)
            .fillna("")
            .agg(" | ".join, axis=1)
            .str.lower()
        )
        filtered = filtered[blob.str.contains(search, na=False, regex=False)].copy()

    if "Status date" in filtered.columns and st.session_state.get("fs_status_date_filter"):
        status_dates = pd.to_datetime(filtered["Status date"], errors="coerce").dt.date
        filtered = filtered.assign(_status_date=status_dates)
        status_date_from = st.session_state.get("fs_status_date_from")
        status_date_to = st.session_state.get("fs_status_date_to")
        if status_date_from:
            filtered = filtered[filtered["_status_date"] >= status_date_from]
        if status_date_to:
            filtered = filtered[filtered["_status_date"] <= status_date_to]
        filtered = filtered.drop(columns=["_status_date"], errors="ignore")

    if "Unique ID" in filtered.columns:
        filtered = filtered.sort_values(by="Unique ID", ascending=False, na_position="last")

    if "FG Due Date" in filtered.columns:
        filtered["FG Due Date"] = (
            pd.to_datetime(filtered["FG Due Date"], errors="coerce")
            .dt.date
            .astype("string")
            .fillna("")
        )

    st.markdown('<div id="fs-table">', unsafe_allow_html=True)
    if edit_mode:
        editable = filtered.copy().reset_index(drop=True)
        if "Delete" not in editable.columns:
            editable["Delete"] = False
        edited = st.data_editor(
            editable,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            height=720,
            key="fs_editor",
            on_change=mark_fs_changes,
        )
    else:
        edited = filtered
        st.dataframe(
            edited,
            use_container_width=True,
            hide_index=True,
            height=720,
        )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        """
        <script>
        const scrollToBottom = () => {
          const grids = window.parent.document.querySelectorAll('[data-testid="stDataFrame"] div[role="grid"]');
          grids.forEach((grid) => { grid.scrollTop = grid.scrollHeight; });
        };
        let attempts = 0;
        const interval = setInterval(() => {
          scrollToBottom();
          attempts += 1;
          if (attempts > 12) {
            clearInterval(interval);
          }
        }, 400);
        </script>
        """,
        unsafe_allow_html=True,
    )

    if save_clicked:
        cleaned = edited
        marked_for_delete = pd.Series(False, index=cleaned.index)
        if "Delete" in cleaned.columns:
            marked_for_delete = cleaned["Delete"].fillna(False)
        if marked_for_delete.any():
            if not st.session_state.get("fs_confirm_delete"):
                st.session_state["fs_confirm_delete"] = True
                st.warning("Rows are marked for deletion. Click save again to confirm.")
                return
            cleaned = cleaned[~marked_for_delete].copy()
        st.session_state["fs_confirm_delete"] = False
        cleaned = cleaned.drop(columns=["Delete"], errors="ignore")
        columns_to_drop = ["#"]
        save_processed_requests(
            FS_PROCESSED_PATH,
            cleaned.drop(columns=columns_to_drop, errors="ignore"),
        )
        st.cache_data.clear()
        st.success("Processed requests saved.")
        st.session_state["fs_pending_changes"] = False
    if st.session_state.get("fs_new_data") is not None:
        st.markdown("### Process New Data")
        st.dataframe(
            st.session_state["fs_new_data"],
            use_container_width=True,
            hide_index=True,
        )
