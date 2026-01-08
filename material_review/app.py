import re
import html
import json
from io import BytesIO
from pathlib import Path
from collections import defaultdict
from datetime import datetime, date

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import pdfplumber
import pyodbc
import FS


# -------------------- CONFIG --------------------
PDF_FOLDER_DEFAULT = r"P:\Planning\BBW\BBW POs\BBW POs 2026"

# FS
FS_MASTER_PATH_DEFAULT = r"P:\Shared\From QC\Fragrance Screening Planning\Master FS.xlsm"
FS_SHEET_NAME = "PROCESSED"
#  A=0 (PO/PO-Line), F=5 (Lot), M=12 (Status)
FS_COL_KEY, FS_COL_LOT, FS_COL_STATUS = 0, 5, 12


FPN_MASTER_PATH_DEFAULT = r"P:\QC\Access Share\First Production Notification & pdf.xlsm"
FPN_SHEET_NAME = "Sheet1"
SCHED_REPORT_PATH_DEFAULT = r"\\PCCSTR\dept\Shared\From QC\Scheduling Report\QC Scheduling Report.xlsm"
SCHED_SHEET_2025 = "2026"
SCHED_SHEET_ARCHIVE = "2022-2023-2024-2025"


INCOMING_BASE_FOLDER = r"P:\Shared\FROM PLANNING\BBW Incoming"

# Incoming: E=4 Article, K=10 ETA, S=18 Qty, V=21 Updates/Tracking
INC_COL_ART, INC_COL_ETA, INC_COL_QTY, INC_COL_UPD = 4, 10, 18, 21

CACHE_DIR = Path(__file__).resolve().parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

LAST_RESULT_FILE = CACHE_DIR / "last_merged.parquet"
LAST_VIEW_FILE = CACHE_DIR / "last_view_snapshot.parquet"  
NOTES_FILE = CACHE_DIR / "planner_notes.json" 
SUPPORT_FILE = CACHE_DIR / "component_support.json"

# change history
CHANGE_LOG_FILE = CACHE_DIR / "change_history.parquet"

#  D=3, E=4, F=5, G=6, K=10, L=11, M=12, O=14, S=18
COL_D_PO, COL_E_LINE, COL_F_ART, COL_G_DESC, COL_K_DD, COL_L_SD, COL_M_BUY_QTY, COL_O_QTY_EA, COL_S_OPEN_QTY = (
    3, 4, 5, 6, 10, 11, 12, 14, 18
)

SKIP_FIRST_DATA_ROW = True
BUCKETS = ["Glass", "WRAP", "BLBL", "LID", "FLBL", "FRG"]


BASE_COLS = ["PO-Line", "Item", "Article", "Description", "DeliveryDate", "StatisticalDate", "QtyEA"]


# -------------------- REGEX --------------------
PO_LINE_HEADER_RE = re.compile(
    r"^\s*(\d{5})\s+(Open|Closed|Cancelled|Partially|Delivered|Partially\s+Delivered)\b",
    re.IGNORECASE,
)
ARTICLE_ANYWHERE_RE = re.compile(r"\b(\d{7,9})\b")
COMPONENT_MARK_RE = re.compile(r"\bQTY\s+PER\s+ASSEMBLY\b", re.IGNORECASE)
QTY_PER_ASSEMBLY_RE = re.compile(r"\bQTY\s+PER\s+ASSEMBLY\b", re.IGNORECASE)
NUM_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)")

AVAIL_NUM_RE = re.compile(r"\bavail\s+([0-9,]+)\b", re.IGNORECASE)
DATE_SLASH_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b")
DATE_DASH_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
PO_NOTES_RE = re.compile(r"\bP\.?O\.?\s*Notes?\b", re.IGNORECASE)
FILLED_CANDLE_RE = re.compile(r"\b[A-Z]{2,}\d{4,}\s+\d+PK\b")
FILLED_CANDLE_LINE_RE = re.compile(r"\bFilled\s+Candle\b", re.IGNORECASE)


# -------------------- HELPERS --------------------
def norm_po(v: str) -> str:
    s = str(v).strip()
    d = re.sub(r"\D", "", s)
    return d or s


def norm_line(v: str) -> str:
    s = str(v).strip()
    d = re.sub(r"\D", "", s)
    if not d:
        return s
    return str(int(d))


def fmt_qty(x):
    if x is None or x == "":
        return ""
    try:
        f = float(str(x).replace(",", ""))
        if abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
        return str(f).rstrip("0").rstrip(".")
    except Exception:
        return str(x)


def parse_csv_list(s: str) -> list[str]:
    if s is None:
        return []
    s = str(s).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def parse_csv_qtys(s: str) -> list[float]:
    out: list[float] = []
    for x in parse_csv_list(s):
        try:
            out.append(float(x.replace(",", "")))
        except Exception:
            out.append(0.0)
    return out


def row_has_components(r: pd.Series) -> bool:
    for b in BUCKETS:
        if str(r.get(b, "")).strip():
            return True
        if str(r.get(f"{b}_Qty", "")).strip():
            return True
    return False


def dismissible_notice(key: str, text: str, kind: str = "warning"):
    state_key = f"dismiss_{key}"
    if state_key not in st.session_state:
        st.session_state[state_key] = False
    if st.session_state[state_key]:
        return

    box = st.container()
    c1, c2 = box.columns([20, 1], vertical_alignment="center")

    with c1:
        if kind == "warning":
            st.warning(text)
        elif kind == "error":
            st.error(text)
        else:
            st.info(text)

    with c2:
        if st.button("✕", key=f"close_{key}", help="Dismiss"):
            st.session_state[state_key] = True
            st.rerun()


def _sanitize_text(value: str) -> str:
    if value is None:
        return ""
    text = str(value)
    for _ in range(3):
        unescaped = html.unescape(text)
        if unescaped == text:
            break
        text = unescaped
    text = re.sub(r"&lt;/?[^&]+&gt;", "", text, flags=re.IGNORECASE)
    text = re.sub(r"</?[^>]+>", "", text)
    text = re.sub(r"</?\s*div\s*>", "", text, flags=re.IGNORECASE)
    return text.replace("<", "").replace(">", "")


def _kv(label: str, value: str) -> str:
    v = html.escape(_sanitize_text(value))
    return f"""
    <div class="kv">
      <div class="k">{label}</div>
      <div class="v">{v}</div>
    </div>
    """


def _to_float(x, default=0.0) -> float:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
        return float(str(x).replace(",", "").strip() or default)
    except Exception:
        return default


def add_serial(df: pd.DataFrame) -> pd.DataFrame:
    """Adds first column '#' starting from 1."""
    df2 = df.reset_index(drop=True).copy()
    df2.insert(0, "#", range(1, len(df2) + 1))
    return df2


def _norm_article(x) -> str:
    s = "" if x is None else str(x).strip()
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\D", "", s)
    return s


def _format_date_only(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return str(dt.date())


def _date_from_text(text: str) -> date | None:
    if not text:
        return None
    text = str(text)
    match = DATE_SLASH_RE.search(text)
    if match:
        try:
            month = int(match.group(1))
            day = int(match.group(2))
            year_raw = match.group(3)
            if year_raw:
                year = int(year_raw)
                if year < 100:
                    year += 2000
            else:
                year = datetime.now().year
            return date(year, month, day)
        except ValueError:
            return None
    match = DATE_DASH_RE.search(text)
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None
    return None

def _extract_qty_per_assembly(line: str) -> float | None:
    if not line:
        return None
    match = QTY_PER_ASSEMBLY_RE.search(line)
    if not match:
        return None
    tail = line[match.end():]
    numbers = [n.replace(",", "") for n in NUM_RE.findall(tail)]
    if not numbers:
        return None
    try:
        return float(numbers[-1])
    except ValueError:
        return None


# -------------------- NOTES (PLANNERS) --------------------
def load_notes() -> dict:
    if not NOTES_FILE.exists():
        return {}
    try:
        obj = json.loads(NOTES_FILE.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            return {str(k).strip(): ("" if v is None else str(v)) for k, v in obj.items()}
    except Exception:
        return {}
    return {}


def save_notes(notes: dict) -> None:
    try:
        NOTES_FILE.write_text(json.dumps(notes, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def load_component_support() -> dict[str, bool]:
    if not SUPPORT_FILE.exists():
        return {}
    try:
        obj = json.loads(SUPPORT_FILE.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            return {str(k).strip(): bool(v) for k, v in obj.items()}
    except Exception:
        return {}
    return {}


def save_component_support(overrides: dict[str, bool]) -> None:
    try:
        SUPPORT_FILE.write_text(json.dumps(overrides, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def apply_component_support_overrides(df: pd.DataFrame, overrides: dict[str, bool]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if not overrides:
        return df
    df2 = df.copy()
    key_map = {str(k).strip(): bool(v) for k, v in overrides.items()}
    mask = df2["PO-Line"].astype(str).map(lambda k: key_map.get(str(k).strip(), False))
    if "Flag" in df2.columns:
        mask = mask & (df2["Flag"].astype(str).str.strip() == "🟪")
    df2.loc[mask, "Short_Detail"] = "Component Supported"
    df2.loc[mask, "Status"] = "OK"
    if "Flag" in df2.columns:
        df2.loc[mask, "Flag"] = "🟩"
    return df2


# -------------------- History tracking  --------------------
TRACK_COLS = [
    "Article",
    "QtyEA",
    "DeliveryDate",
    "StatisticalDate",
    "Status",
    "Short_Detail",
    "FS_Lot",
    "FS_Status",
]
CHANGE_VIEW_COLS = ["PO-Line"] + TRACK_COLS


def _safe_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
        return ""
    return str(x).strip()


def load_last_view_snapshot() -> pd.DataFrame | None:
    if LAST_VIEW_FILE.exists():
        try:
            return pd.read_parquet(LAST_VIEW_FILE)
        except Exception:
            try:
                return pd.read_pickle(LAST_VIEW_FILE.with_suffix(".pkl"))
            except Exception:
                return None
    pkl = LAST_VIEW_FILE.with_suffix(".pkl")
    if pkl.exists():
        try:
            return pd.read_pickle(pkl)
        except Exception:
            return None
    return None


def persist_view_snapshot(df_view: pd.DataFrame) -> None:
    try:
        df_view.to_parquet(LAST_VIEW_FILE, index=False)
    except Exception:
        try:
            df_view.to_pickle(LAST_VIEW_FILE.with_suffix(".pkl"))
        except Exception:
            pass


def _sum_avail_from_short_detail(s: str) -> float:
    if not s:
        return 0.0
    nums = []
    for m in AVAIL_NUM_RE.finditer(str(s)):
        try:
            nums.append(float(m.group(1).replace(",", "")))
        except Exception:
            pass
    return float(sum(nums)) if nums else 0.0


def _label_change(field: str, oldv: str, newv: str) -> str:
    f = (field or "").strip()

    if f in ("FS_Status", "FS_Lot"):
        return "FS changed"

    if f == "Status":
        return "Status changed"

    if f == "Short_Detail":
        old_sum = _sum_avail_from_short_detail(oldv)
        new_sum = _sum_avail_from_short_detail(newv)
        if old_sum > 0 or new_sum > 0:
            if new_sum > old_sum:
                return "Availability increased"
            if new_sum < old_sum:
                return "Availability decreased"
        return "Short detail changed"

    if f in ("DeliveryDate", "StatisticalDate"):
        return "Date changed"

    if f == "QtyEA":
        return "Qty changed"

    if f == "Article":
        return "Article changed"

    return f"{f} changed" if f else "Changed"


def compute_change_log(
    current_view: pd.DataFrame,
    prev_view: pd.DataFrame | None,
    run_ts: datetime,
) -> pd.DataFrame:
    if prev_view is None or prev_view.empty:
        return pd.DataFrame(columns=["RunTS", "PO-Line", "Field", "Old", "New", "Description"])

    if "PO-Line" not in prev_view.columns or "PO-Line" not in current_view.columns:
        return pd.DataFrame(columns=["RunTS", "PO-Line", "Field", "Old", "New", "Description"])

    prev = prev_view.copy()
    cur = current_view.copy()

    prev_keep = [c for c in CHANGE_VIEW_COLS if c in prev.columns]
    cur_keep = [c for c in CHANGE_VIEW_COLS if c in cur.columns]
    prev = prev[prev_keep].copy()
    cur = cur[cur_keep].copy()

    prev_map = {str(r["PO-Line"]).strip(): r for _, r in prev.iterrows()}
    cur_map = {str(r["PO-Line"]).strip(): r for _, r in cur.iterrows()}
    keys = sorted(set(prev_map.keys()) & set(cur_map.keys()))

    rows = []
    for k in keys:
        old_row = prev_map[k]
        new_row = cur_map[k]
        for c in TRACK_COLS:
            if c not in cur.columns or c not in prev.columns:
                continue
            oldv = _safe_str(old_row.get(c, ""))
            newv = _safe_str(new_row.get(c, ""))
            if oldv != newv:
                rows.append(
                    {
                        "RunTS": run_ts,
                        "PO-Line": k,
                        "Field": c,
                        "Old": oldv,
                        "New": newv,
                        "Description": _label_change(c, oldv, newv),
                    }
                )

    df_log = pd.DataFrame(rows)
    if df_log.empty:
        return pd.DataFrame(columns=["RunTS", "PO-Line", "Field", "Old", "New", "Description"])
    return df_log


def load_change_history() -> pd.DataFrame:
    if CHANGE_LOG_FILE.exists():
        try:
            df = pd.read_parquet(CHANGE_LOG_FILE)
            if "ChangeLabel" in df.columns and "Description" not in df.columns:
                df = df.rename(columns={"ChangeLabel": "Description"})
            if "RunTS" in df.columns:
                df["RunTS"] = pd.to_datetime(df["RunTS"], errors="coerce")
            return df
        except Exception:
            try:
                df = pd.read_pickle(CHANGE_LOG_FILE.with_suffix(".pkl"))
                if "ChangeLabel" in df.columns and "Description" not in df.columns:
                    df = df.rename(columns={"ChangeLabel": "Description"})
                if "RunTS" in df.columns:
                    df["RunTS"] = pd.to_datetime(df["RunTS"], errors="coerce")
                return df
            except Exception:
                return pd.DataFrame(columns=["RunTS", "PO-Line", "Field", "Old", "New", "Description"])
    return pd.DataFrame(columns=["RunTS", "PO-Line", "Field", "Old", "New", "Description"])


def append_change_history(df_new: pd.DataFrame) -> None:
    if df_new is None or df_new.empty:
        return
    try:
        existing = load_change_history()
        out = pd.concat([existing, df_new], ignore_index=True)
        out["RunTS"] = pd.to_datetime(out["RunTS"], errors="coerce")
        out = out.sort_values(["RunTS", "PO-Line", "Field"], kind="stable")
        out.to_parquet(CHANGE_LOG_FILE, index=False)
    except Exception:
        try:
            existing = load_change_history()
            out = pd.concat([existing, df_new], ignore_index=True)
            out["RunTS"] = pd.to_datetime(out["RunTS"], errors="coerce")
            out = out.sort_values(["RunTS", "PO-Line", "Field"], kind="stable")
            out.to_pickle(CHANGE_LOG_FILE.with_suffix(".pkl"))
        except Exception:
            pass


def changes_to_excel_bytes(df_changes: pd.DataFrame) -> BytesIO:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df_changes.to_excel(writer, index=False, sheet_name="Changes")
    bio.seek(0)
    return bio


# -------------------- UI STYLE --------------------
BASE_CSS = """
<style>
section[data-testid="stSidebar"] { background: #0b0f19; }
section[data-testid="stSidebar"] * { color: #e5e7eb; }

section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] textarea {
    background: #111827 !important;
    color: #e5e7eb !important;
    border: 1px solid #243244 !important;
}

section[data-testid="stSidebar"] hr {
    margin: 0.35rem 0 !important;
}

section[data-testid="stSidebar"] [data-testid="stSelectbox"] div[data-baseweb="select"] {
    background: #111827 !important;
    color: #e5e7eb !important;
    border: 1px solid #243244 !important;
}

section[data-testid="stSidebar"] [data-testid="stSelectbox"] input {
    color: #e5e7eb !important;
}

section[data-testid="stSidebar"] [data-testid="stSelectbox"] svg {
    fill: #e5e7eb !important;
}

section[data-testid="stSidebar"] [data-testid="stSelectbox"] div[data-baseweb="select"] > div {
    color: #e5e7eb !important;
}

section[data-testid="stSidebar"] [data-testid="stSelectbox"] span {
    color: #e5e7eb !important;
}

section[data-testid="stSidebar"] [data-baseweb="menu"] {
    background: #111827 !important;
    color: #e5e7eb !important;
}

section[data-testid="stSidebar"] [data-baseweb="menu"] * {
    color: #e5e7eb !important;
}

section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {
    background: #111827 !important;
    border: 1px solid #374151 !important;
}
section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] * {
    color: #e5e7eb !important;
    opacity: 1 !important;
}
section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"]:hover {
    border-color: #60a5fa !important;
}

section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button {
    background: #1f2933 !important;
    color: #ffffff !important;
    border: 1px solid #4b5563 !important;
    opacity: 1 !important;
}
section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button:hover {
    background: #111827 !important;
    border-color: #60a5fa !important;
}

section[data-testid="stSidebar"] button[kind="primary"] {
    background: #ef4444 !important;
    border: none !important;
    color: #fff !important;
}

html, body, [data-testid="stApp"] { margin: 0 !important; padding: 0 !important; }
div.block-container {
  padding-top: 0.4rem !important;
  padding-bottom: 0rem !important;
  padding-left: 0.2rem !important;
  padding-right: 0.2rem !important;
  max-width: 100% !important;
}
section.main > div { padding-left: 0rem !important; padding-right: 0rem !important; }

[data-testid="stDataFrame"] [data-testid="stElementToolbar"] { display: none !important; }
[data-testid="stTable"] [data-testid="stElementToolbar"] { display: none !important; }

header { height: 2.2rem !important; min-height: 2.2rem !important; }
header [data-testid="stToolbar"] { height: 2.2rem !important; }

#iconbar{
  display:flex;
  align-items:center;
  justify-content:flex-end;
  gap:6px;
  margin-top:-6px;
}
#iconbar .stButton > button,
#iconbar .stDownloadButton > button,
#iconbar .stPopover > button{
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
#iconbar .stButton > button:hover,
#iconbar .stDownloadButton > button:hover,
#iconbar .stPopover > button:hover{
  border-color:#cbd5e1 !important;
}

#iconbar .mini-icon .stButton > button{
  width: 34px !important;
  height: 34px !important;
  min-width: 34px !important;
  max-width: 34px !important;
  border-radius: 12px !important;
  font-size: 18px !important;
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
  line-height: 1 !important;
  padding: 0 !important;
}

.card{
  border: 1px solid #d1d5db;
  border-radius: 14px;
  padding: 12px 14px;
  margin-bottom: 10px;
  background: #f9fafb;
  box-shadow: 0 6px 18px rgba(15, 23, 42, 0.08);
}
.card h4{
  margin: 0 0 10px 0;
  font-size: 14px;
  font-weight: 700;
  color: #0f172a;
}
.kv{
  display: grid;
  grid-template-columns: 140px 1fr;
  gap: 8px;
  padding: 4px 0;
  border-bottom: 1px dashed #eef2f7;
}
.kv:last-child{ border-bottom:none; }
.k{ color:#6b7280; font-size: 12px; }
.v{ color:#111827; font-size: 12px; font-weight: 600; }
.small{ color:#6b7280; font-size: 12px; }

div[data-testid="stVerticalBlock"]:has(#po-detail-card){
  border: 1px solid #d1d5db;
  border-radius: 14px;
  padding: 12px 14px;
  margin-bottom: 10px;
  background: #f9fafb;
  box-shadow: 0 6px 18px rgba(15, 23, 42, 0.08);
}
div[data-testid="stVerticalBlock"]:has(#po-detail-card) .po-card-title{
  margin: 0 0 10px 0;
  font-size: 14px;
  font-weight: 700;
  color: #0f172a;
}
div[data-testid="stVerticalBlock"]:has(#po-detail-card) .po-card-row{
  display: grid;
  grid-template-columns: 140px 1fr;
  gap: 8px;
  padding: 4px 0;
  border-bottom: 1px dashed #eef2f7;
}
div[data-testid="stVerticalBlock"]:has(#po-detail-card) .po-card-row:last-of-type{
  border-bottom: none;
}
div[data-testid="stVerticalBlock"]:has(#po-detail-card) .po-card-label{
  color: #6b7280;
  font-size: 12px;
}
div[data-testid="stVerticalBlock"]:has(#po-detail-card) .po-card-value{
  color: #111827;
  font-size: 12px;
  font-weight: 600;
}
div[data-testid="stVerticalBlock"]:has(#po-detail-card) .po-card-notes-label{
  margin-top: 10px;
  color: #0f172a;
  font-size: 12px;
  font-weight: 700;
}


[data-testid="stDataFrame"] div[role="grid"] { font-size: 12px; }
</style>
"""

SCROLL_MAIN = """
<style>
[data-testid="stAppViewContainer"] { overflow: hidden !important; }
[data-testid="stAppViewContainer"] > .main { overflow: hidden !important; }
section.main { overflow: hidden !important; }
div.block-container { height: 100vh !important; overflow: hidden !important; }
</style>
"""

SCROLL_DETAILS = """
<style>
[data-testid="stAppViewContainer"] { overflow: auto !important; }
[data-testid="stAppViewContainer"] > .main { overflow: auto !important; }
section.main { overflow: auto !important; }
div.block-container { height: auto !important; overflow: visible !important; }
</style>
"""

SEARCH_CSS = """
<style>
#corner-search { position: relative; width: 100%; }
#corner-search input {
  height: 34px !important;
  padding: 4px 42px 4px 10px !important;
  border-radius: 12px !important;
  font-size: 12px !important;
}

button#btn_clear_search {
  position: absolute !important;
  right: 6px !important;
  top: 50% !important;
  transform: translateY(-50%) !important;

  width: 28px !important;
  height: 28px !important;
  min-width: 28px !important;
  padding: 0 !important;

  border-radius: 8px !important;
  background: #ffffff !important;
  border: 1px solid #d1d5db !important;
  color: #111827 !important;
  line-height: 1 !important;
}

div[data-testid="stButton"]:has(> button#btn_clear_search) {
  position: absolute !important;
  right: 6px !important;
  top: 50% !important;
  transform: translateY(-50%) !important;
  margin: 0 !important;
  padding: 0 !important;
}
</style>
"""


# -------------------- LIVE CLOCK --------------------
def sidebar_clock():
    components.html(
        """
        <div id="clock" style="font-family: system-ui; line-height:1.25; color:#e5e7eb; background:transparent;">
          <div id="clock-day" style="font-size:22px; font-weight:700;">Loading...</div>
          <div id="clock-time" style="font-size:15px;"></div>
        </div>
        <script>
          function pad(n){ return n < 10 ? '0' + n : n; }
          function tick(){
            const d = new Date();
            const days = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];
            let h = d.getHours();
            const ampm = h >= 12 ? "PM" : "AM";
            h = h % 12 || 12;
            document.getElementById("clock-day").innerText = days[d.getDay()];
            document.getElementById("clock-time").innerText =
              `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} — ${pad(h)}:${pad(d.getMinutes())}:${pad(d.getSeconds())} ${ampm}`;
          }
          tick();
          setInterval(tick, 1000);
        </script>
        """,
        height=70,
    )


# -------------------- EXCEL READ --------------------
def read_excel_sheet1(file_name: str, file_buf) -> pd.DataFrame:
    b = bytes(file_buf)
    bio = BytesIO(b)

    if b[:2] == b"PK":
        return pd.read_excel(bio, sheet_name=0, engine="openpyxl", header=None)

    if b[:4] == b"\xD0\xCF\x11\xE0":
        return pd.read_excel(bio, sheet_name=0, engine="xlrd", header=None)

    head = b[:800].decode("latin-1", errors="ignore").upper()
    if "MIME-VERSION" in head or "<HTML" in head or "<TABLE" in head:
        html = b.decode("latin-1", errors="ignore")
        tables = pd.read_html(BytesIO(html.encode("utf-8")), header=None)
        if not tables:
            raise ValueError(f"{file_name}: HTML detected but no tables found.")
        return tables[0]

    return pd.read_excel(bio, sheet_name=0, header=None)


def combine_excels(excel_files) -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    need = max(
        COL_D_PO,
        COL_E_LINE,
        COL_F_ART,
        COL_G_DESC,
        COL_K_DD,
        COL_L_SD,
        COL_M_BUY_QTY,
        COL_O_QTY_EA,
        COL_S_OPEN_QTY,
    )
    frames: list[pd.DataFrame] = []
    skipped: list[tuple[str, str]] = []

    for f in excel_files:
        try:
            df = read_excel_sheet1(f.name, f.getbuffer())
        except Exception as e:
            skipped.append((f.name, f"Failed to read: {e}"))
            continue

        if df.shape[1] <= need:
            skipped.append((f.name, f"Not enough columns: got {df.shape[1]}, need at least {need+1}"))
            continue

        if SKIP_FIRST_DATA_ROW and len(df) > 0:
            df = df.iloc[1:].copy()

        po = df.iloc[:, COL_D_PO].astype(str).str.strip()
        line = df.iloc[:, COL_E_LINE].astype(str).str.strip()

        buy_qty = df.iloc[:, COL_M_BUY_QTY].map(_to_float)
        qty_ea = df.iloc[:, COL_O_QTY_EA].map(_to_float)
        open_qty = df.iloc[:, COL_S_OPEN_QTY].map(_to_float)
        qty_pcs = qty_ea.where(buy_qty != 0, 0).div(buy_qty.where(buy_qty != 0, 1)).mul(open_qty)

        out = pd.DataFrame(
            {
                "PO-Line": po + "-" + line,
                "PO_norm": po.map(norm_po),
                "Line_norm": line.map(norm_line),
                "Article": df.iloc[:, COL_F_ART],
                "Description": df.iloc[:, COL_G_DESC],
                "DeliveryDate": df.iloc[:, COL_K_DD],
                "StatisticalDate": df.iloc[:, COL_L_SD],
                "QtyEA": qty_pcs.map(fmt_qty),
                "OpenQty": open_qty,
            }
        )

        out = out[out["PO_norm"].astype(str).str.len() > 0]
        out = out[open_qty > 0]
        out = out.drop_duplicates(subset=["PO-Line"])
        frames.append(out)

    if not frames:
        raise ValueError("No valid Excel files. Please export using the correct report format.")

    merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["PO-Line"])
    return merged, skipped


# -------------------- PDF PARSING --------------------
def classify_component(desc: str):
    d = (desc or "").upper()

    if "POLYSHEET" in d and "LID" in d:
        return None
    if "POLYSHEET" in d and "GLASS" in d:
        return None

    if ("SBA" in d) or ("SHRINK" in d) or ("SLEEVE" in d) or re.search(r"\bSLV\b", d):
        return "WRAP"

    if "FLBL" in d or "WLBL" in d:
        return "FLBL"
    if "BLBL" in d:
        return "BLBL"

    if re.search(r"\bLID\b", d):
        return "LID"

    if ("CYLINDER" in d and "GLASS" in d) or re.search(r"\bGLASS\b", d):
        return "Glass"

    if "FRAG" in d or re.search(r"\bFRG\b", d):
        return "FRG"

    return None


def parse_po_pdf_by_line(pdf_path: Path):
    per_line = defaultdict(lambda: {k: [] for k in BUCKETS})
    current_line_norm = None

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if not text.strip():
                continue
            for raw in text.splitlines():
                s = raw.strip()
                if not s:
                    continue

                hm = PO_LINE_HEADER_RE.match(s)
                if hm:
                    current_line_norm = norm_line(hm.group(1))
                    continue

                if not current_line_norm:
                    continue
                if not COMPONENT_MARK_RE.search(s):
                    continue

                bucket = classify_component(s)
                if not bucket:
                    continue

                m_art = ARTICLE_ANYWHERE_RE.search(s)
                if not m_art:
                    continue

                qty_per = _extract_qty_per_assembly(s)

                item = (m_art.group(1), qty_per)
                if item not in per_line[current_line_norm][bucket]:
                    per_line[current_line_norm][bucket].append(item)

    line_keys = list(per_line.keys())
    if len(line_keys) > 1:
        for bucket in BUCKETS:
            shared_items = []
            for ln in line_keys:
                for itm in per_line[ln][bucket]:
                    if itm not in shared_items:
                        shared_items.append(itm)
            if not shared_items:
                continue
            for ln in line_keys:
                if not per_line[ln][bucket]:
                    per_line[ln][bucket] = shared_items.copy()

    return per_line


def parse_po_pdf_meta(pdf_path: Path) -> dict:
    po_notes = ""
    filled_candle = ""

    with pdfplumber.open(str(pdf_path)) as pdf:
        lines: list[str] = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            if not text.strip():
                continue
            for raw in text.splitlines():
                s = raw.strip()
                if s:
                    lines.append(s)

        for idx, line in enumerate(lines):
            if not po_notes and PO_NOTES_RE.search(line):
                parts = re.split(r":", line, maxsplit=1)
                if len(parts) > 1 and parts[1].strip():
                    po_notes = parts[1].strip()
                else:
                    for nxt in lines[idx + 1 :]:
                        if nxt.strip():
                            po_notes = nxt.strip()
                            break

            if not filled_candle and FILLED_CANDLE_LINE_RE.search(line):
                m_art = ARTICLE_ANYWHERE_RE.search(line)
                if m_art:
                    filled_candle = m_art.group(1).strip()
                else:
                    m = FILLED_CANDLE_RE.search(line)
                    if m:
                        filled_candle = m.group(0).strip()

            if po_notes and filled_candle:
                break

    return {"PO_Notes": po_notes, "Filled_Candle": filled_candle}


def build_pdf_index(folder: str) -> dict:
    fp = Path(folder)
    if not fp.exists():
        return {}

    idx: dict[str, str] = {}
    best: dict[str, Path] = {}

    for p in fp.glob("*.pdf"):
        m = re.search(r"(\d{6,12})", p.stem)
        if not m:
            continue
        po = m.group(1)
        cur = best.get(po)
        if cur is None or p.stat().st_mtime > cur.stat().st_mtime:
            best[po] = p

    for po, p in best.items():
        idx[po] = str(p)

    return idx


def pdf_folder_mtime(folder: str) -> float:
    fp = Path(folder)
    if not fp.exists():
        return 0.0
    mtimes = [p.stat().st_mtime for p in fp.glob("*.pdf")]
    return max(mtimes, default=0.0)


@st.cache_data(show_spinner=False)
def extract_pdf_map(folder: str, needed_pos: tuple[str, ...], folder_mtime: float) -> dict:
    idx = build_pdf_index(folder)
    out: dict[tuple[str, str], dict] = {}

    for po in needed_pos:
        pdf_path = idx.get(str(po))
        if not pdf_path:
            continue

        per_line = parse_po_pdf_by_line(Path(pdf_path))
        for line_norm, buckets in per_line.items():
            out[(str(po), str(line_norm))] = buckets

    return out


@st.cache_data(show_spinner=False)
def extract_pdf_meta_map(folder: str, needed_pos: tuple[str, ...], folder_mtime: float) -> dict[str, dict]:
    idx = build_pdf_index(folder)
    out: dict[str, dict] = {}

    for po in needed_pos:
        pdf_path = idx.get(str(po))
        if not pdf_path:
            continue
        out[str(po)] = parse_po_pdf_meta(Path(pdf_path))

    return out


def merge(excel_df: pd.DataFrame, pdf_map: dict, pdf_meta_map: dict | None = None) -> pd.DataFrame:
    df = excel_df.copy()
    df["PO_norm"] = df["PO_norm"].astype(str)
    df["Line_norm"] = df["Line_norm"].astype(str)

    keys = list(zip(df["PO_norm"].tolist(), df["Line_norm"].tolist()))

    for b in BUCKETS:
        arts_col = []
        qtys_col = []
        per_col = []
        for i, k in enumerate(keys):
            items = (pdf_map.get(k, {}) or {}).get(b, []) or []
            qty_ea = _to_float(df.loc[i, "QtyEA"]) if "QtyEA" in df.columns else 0.0
            arts_col.append(", ".join([a for a, _q in items]))
            per_col.append(", ".join([fmt_qty(q) for _a, q in items if q is not None]))
            scaled = []
            for _a, qty_per in items:
                if qty_per is None:
                    continue
                scaled.append(fmt_qty(float(qty_per) * qty_ea))
            qtys_col.append(", ".join([q for q in scaled if q]))
        df[b] = arts_col
        df[f"{b}_Qty"] = qtys_col
        df[f"{b}_Per"] = per_col

    if pdf_meta_map:
        df["PO_Notes"] = df["PO_norm"].map(lambda k: (pdf_meta_map.get(str(k), {}) or {}).get("PO_Notes", ""))
        df["Filled_Candle"] = df["PO_norm"].map(
            lambda k: (pdf_meta_map.get(str(k), {}) or {}).get("Filled_Candle", "")
        )
    else:
        df["PO_Notes"] = ""
        df["Filled_Candle"] = ""

    return df


# -------------------- PERSIST --------------------
def persist_last(df: pd.DataFrame):
    try:
        df.to_parquet(LAST_RESULT_FILE, index=False)
    except Exception:
        df.to_pickle(LAST_RESULT_FILE.with_suffix(".pkl"))


def load_last():
    if LAST_RESULT_FILE.exists():
        try:
            return pd.read_parquet(LAST_RESULT_FILE)
        except Exception:
            pass
    pkl = LAST_RESULT_FILE.with_suffix(".pkl")
    if pkl.exists():
        try:
            return pd.read_pickle(pkl)
        except Exception:
            return None
    return None


# -------------------- FRAGRANCE SCREENING (FS) --------------------
def _parse_fs_key(raw) -> tuple[str, str]:
    s = "" if raw is None else str(raw).strip()
    if not s:
        return "", ""

    if "-" in s:
        left, right = s.split("-", 1)
        po = norm_po(left)
        ln = norm_line(right)
        return po, ln

    po = norm_po(s)
    return po, ""


@st.cache_data(show_spinner=False)
def load_fs_master(fs_path: str) -> dict:
    fp = Path(fs_path)
    if not fp.exists():
        return {"by_pair": {}, "by_po": {}}

    df = pd.read_excel(fp, sheet_name=FS_SHEET_NAME, engine="openpyxl", header=None)

    if df.shape[1] <= max(FS_COL_KEY, FS_COL_LOT, FS_COL_STATUS):
        return {"by_pair": {}, "by_po": {}}

    by_pair: dict[tuple[str, str], dict] = {}
    by_po: dict[str, dict] = {}

    for _, r in df.iterrows():
        key_raw = r.iloc[FS_COL_KEY]
        lot = r.iloc[FS_COL_LOT] if FS_COL_LOT < len(r) else None
        status = r.iloc[FS_COL_STATUS] if FS_COL_STATUS < len(r) else None

        po, ln = _parse_fs_key(key_raw)
        if not po:
            continue

        rec = {
            "FS_Lot": "" if pd.isna(lot) else str(lot).strip(),
            "FS_Status": "" if pd.isna(status) else str(status).strip(),
        }

        if ln:
            by_pair[(po, ln)] = rec
        else:
            by_po[po] = rec

    return {"by_pair": by_pair, "by_po": by_po}


def fs_lookup_for_row(r: pd.Series, fs_maps: dict) -> dict:
    if not str(r.get("FRG", "")).strip() and not str(r.get("FRG_Qty", "")).strip():
        return {"FS_Lot": "", "FS_Status": ""}

    po = str(r.get("PO_norm", "")).strip()
    ln = str(r.get("Line_norm", "")).strip()

    by_pair = fs_maps.get("by_pair", {}) if isinstance(fs_maps, dict) else {}
    by_po = fs_maps.get("by_po", {}) if isinstance(fs_maps, dict) else {}

    rec = by_pair.get((po, ln))
    if rec:
        return rec

    rec2 = by_po.get(po)
    if rec2:
        return rec2

    return {"FS_Lot": "", "FS_Status": ""}


def format_fs_info(fs_lot: str, fs_status: str) -> str:
    fs_lot = "" if fs_lot is None else str(fs_lot).strip()
    fs_status = "" if fs_status is None else str(fs_status).strip()
    if not fs_lot and not fs_status:
        return ""
    parts = []
    if fs_lot:
        parts.append(f"Lot {fs_lot}")
    if fs_status:
        parts.append(fs_status)
    return "FS: " + ", ".join(parts)


# -------------------- INCOMING (FAST + CACHED) --------------------
def _find_current_month_folder(base: str) -> Path | None:
    basep = Path(base)
    if not basep.exists():
        return None

    y = datetime.now().year
    yp = basep / str(y)
    if not yp.exists():
        return None

    month_num = datetime.now().strftime("%m")
    month_name = datetime.now().strftime("%B").lower()

    subdirs = [d for d in yp.iterdir() if d.is_dir()]
    if not subdirs:
        return None

    matches = []
    for d in subdirs:
        n = d.name.lower()
        if n.startswith(month_num) or (month_num in n) or (month_name in n):
            matches.append(d)

    candidates = matches if matches else subdirs
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _find_latest_incoming_file(month_dir: Path) -> Path | None:
    if not month_dir or not month_dir.exists():
        return None
    files = []
    for ext in ("*.xlsx", "*.xlsm", "*.xls"):
        files.extend(month_dir.glob(ext))
    files = [f for f in files if not f.name.startswith("~$")]
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


@st.cache_data(show_spinner=False)
def load_incoming_map(latest_file_path: str, mtime: float) -> dict:
    p = Path(latest_file_path)
    if not p.exists():
        return {}

    try:
        df = pd.read_excel(p, sheet_name=0, engine="openpyxl", header=None)
    except PermissionError:
        return {}
    except OSError:
        return {}

    need_cols = max(INC_COL_ART, INC_COL_ETA, INC_COL_QTY, INC_COL_UPD)
    if df.shape[1] <= need_cols:
        return {}

    art_s = df.iloc[:, INC_COL_ART].astype(str).str.strip()
    eta_s = df.iloc[:, INC_COL_ETA]
    qty_s = df.iloc[:, INC_COL_QTY]
    upd_s = df.iloc[:, INC_COL_UPD]

    out: dict[str, list[dict]] = {}
    for a, eta, q, u in zip(art_s, eta_s, qty_s, upd_s):
        a2 = _norm_article(a)
        if not a2:
            continue

        qf = _to_float(q, default=0.0)
        upd = "" if (u is None or (isinstance(u, float) and pd.isna(u))) else str(u).strip()
        eta_text = _format_date_only(eta)

        shipment = {
            "qty": qf,
            "updates": upd,
            "eta": eta_text,
        }
        out.setdefault(a2, []).append(shipment)

    return out


def get_incoming_context() -> tuple[dict, str]:
    month_dir = _find_current_month_folder(INCOMING_BASE_FOLDER)
    if not month_dir:
        return {}, ""
    lf = _find_latest_incoming_file(month_dir)
    if not lf:
        return {}, ""
    mtime = lf.stat().st_mtime
    mp = load_incoming_map(str(lf), mtime)
    return mp, lf.name


def incoming_text_for_article(art: str, incoming_map: dict, needed_qty: float | None = None) -> str:
    a = _norm_article(art)
    if not a:
        return ""
    shipments = incoming_map.get(a)
    if not shipments:
        return ""

    remaining = None
    if needed_qty is not None:
        remaining = max(float(needed_qty or 0.0), 0.0)

    parts = []
    for shipment in shipments:
        qty = float(shipment.get("qty", 0.0) or 0.0)
        upd = str(shipment.get("updates", "") or "").strip()
        eta = str(shipment.get("eta", "") or "").strip()
        if qty <= 1e-9 and not upd and not eta:
            continue
        if remaining is not None:
            if remaining <= 1e-9:
                break
            remaining -= max(qty, 0.0)
        segs = []
        if qty > 1e-9:
            segs.append(fmt_qty(qty))
        updates_date = _date_from_text(upd)
        if updates_date:
            segs.append(upd)
        elif eta:
            segs.append(f"ETA {eta}")
        elif upd:
            segs.append(upd)
        if segs:
            parts.append(" ".join(segs))
        if remaining is not None and remaining <= 1e-9:
            break

    if not parts:
        return ""
    return "Incoming: " + " | ".join(parts)


def _incoming_matches_date(updates: str, eta: str, target_date: date) -> bool:
    updates_date = _date_from_text(updates)
    if updates_date:
        return updates_date == target_date
    eta_date = _date_from_text(eta)
    if eta_date:
        return eta_date == target_date
    return False


# -------------------- FILLED CANDLE LOGIC --------------------
def _coerce_sortable(series: pd.Series) -> tuple[pd.Series, str]:
    for converter, label in (
        (pd.to_datetime, "datetime"),
        (pd.to_numeric, "numeric"),
    ):
        try:
            converted = converter(series, errors="coerce")
            if converted.notna().any():
                return converted, label
        except Exception:
            continue
    return series.astype(str), "string"


def _xlookup_prev(value, lookup_series: pd.Series, return_series: pd.Series):
    lookup_series = lookup_series.dropna()
    return_series = return_series.loc[lookup_series.index]
    if lookup_series.empty:
        return ""

    lookup_conv, lookup_kind = _coerce_sortable(lookup_series)
    return_conv, _ = _coerce_sortable(return_series)
    if lookup_kind == "datetime":
        val = pd.to_datetime(value, errors="coerce")
    elif lookup_kind == "numeric":
        val = pd.to_numeric(value, errors="coerce")
    else:
        val = str(value) if value is not None else ""
    if pd.isna(val):
        return ""

    ordered = lookup_conv.sort_values()
    idx = ordered.searchsorted(val, side="right") - 1
    if idx < 0:
        return ""

    matched_index = ordered.index[int(idx)]
    return return_conv.loc[matched_index]


@st.cache_data(show_spinner=False)
def load_fpn_lookup(path: str) -> pd.DataFrame:
    fp = Path(path)
    if not fp.exists():
        return pd.DataFrame(columns=["FilledCandle", "ResultK"])
    df = pd.read_excel(fp, sheet_name=FPN_SHEET_NAME, engine="openpyxl", header=None)
    if df.shape[1] < 2:
        return pd.DataFrame(columns=["FilledCandle", "ResultK"])
    out = pd.DataFrame(
        {
            "FilledCandle": df.iloc[:, 0].astype(str).str.strip(),
            "ResultK": df.iloc[:, 1],
        }
    )
    return out[out["FilledCandle"].astype(str).str.len() > 0].drop_duplicates(subset=["FilledCandle"])


@st.cache_data(show_spinner=False)
def load_sched_lookup(path: str) -> dict[str, pd.DataFrame]:
    fp = Path(path)
    if not fp.exists():
        return {}

    out = {}
    for sheet in (SCHED_SHEET_2025, SCHED_SHEET_ARCHIVE):
        try:
            df = pd.read_excel(fp, sheet_name=sheet, engine="openpyxl", header=None)
        except Exception:
            continue
        if df.shape[1] < 9:
            continue
        out[sheet] = pd.DataFrame(
            {
                "LookupI": df.iloc[:, 8],
                "ReturnA": df.iloc[:, 0],
            }
        )
    return out


# -------------------- INVENTORY SQL --------------------
SQL_STOCK_BY_ARTICLE = """
SELECT
    MAX(METHDM.AQMTLP) AS Item,
    STKMP.AWVPT# AS Article,
    MAX(METHDM.AQMTLD) AS Description,
    COALESCE(STOCK.TotalQOH, 0) AS QOH,
    COALESCE(STOCK.TotalAlloc, 0) AS Allocation,
    COALESCE(STOCK.QCIQty, 0) AS QCHold_QCI,
    COALESCE(STOCK.QCHQty, 0) AS QCHold_QCH,
    (COALESCE(STOCK.TotalQOH,0)
     - COALESCE(STOCK.TotalAlloc,0)
     - COALESCE(STOCK.QCIQty,0)
     - COALESCE(STOCK.QCHQty,0)) AS Variant
FROM IVPDAT.METHDM METHDM
LEFT JOIN IVPDAT.STKMP STKMP
    ON METHDM.AQMTLP = STKMP.AWPART
LEFT JOIN (
    SELECT
        BXPART,
        SUM(BXQTOH) AS TotalQOH,
        SUM(BXQTAL) AS TotalAlloc,
        SUM(CASE WHEN BXSTOK = 'PCCQCI' THEN BXQTOH ELSE 0 END) AS QCIQty,
        SUM(CASE WHEN BXSTOK = 'PCCQCH' THEN BXQTOH ELSE 0 END) AS QCHQty
    FROM IVPDAT.STKB
    GROUP BY BXPART
) STOCK
    ON METHDM.AQMTLP = STOCK.BXPART
WHERE STKMP.AWVPT# IN ({placeholders})
GROUP BY
    STKMP.AWVPT#,
    STOCK.TotalQOH,
    STOCK.TotalAlloc,
    STOCK.QCIQty,
    STOCK.QCHQty
"""

SQL_STOCK_BY_ARTICLE_FALLBACK = """
SELECT
    STKMM.AVPART AS Item,
    STKMM.AVDES3 AS Article,
    STKMM.AVDES1,
    STKMM.AVDES2,
    COALESCE(STOCK.TotalQOH, 0) AS QOH,
    COALESCE(STOCK.TotalAlloc, 0) AS Allocation,
    COALESCE(STOCK.QCIQty, 0) AS QCHold_QCI,
    COALESCE(STOCK.QCHQty, 0) AS QCHold_QCH,
    (COALESCE(STOCK.TotalQOH,0)
     - COALESCE(STOCK.TotalAlloc,0)
     - COALESCE(STOCK.QCIQty,0)
     - COALESCE(STOCK.QCHQty,0)) AS Variant
FROM IVPDAT.STKMM STKMM
LEFT JOIN (
    SELECT
        BXPART,
        SUM(BXQTOH) AS TotalQOH,
        SUM(BXQTAL) AS TotalAlloc,
        SUM(CASE WHEN BXSTOK = 'PCCQCI' THEN BXQTOH ELSE 0 END) AS QCIQty,
        SUM(CASE WHEN BXSTOK = 'PCCQCH' THEN BXQTOH ELSE 0 END) AS QCHQty
    FROM IVPDAT.STKB
    GROUP BY BXPART
) STOCK
    ON STKMM.AVPART = STOCK.BXPART
WHERE STKMM.AVDES3 IN ({placeholders})
"""

SQL_DASHBOARD_RECEIVING = """
SELECT 
    TRIM(P."JSPT#") AS Part,
    '(' || TRIM(COALESCE(S.AWVPT#, '')) || ')' || TRIM(P.JSPDES) AS Description,
    P.JSQTYR AS ReceivedQty,
    P.JSOUNT AS UOM,
    P.JSRDAT AS ReceiptDate,
    P.JSPSLP AS PO,
    POH.KAOVNM AS Vendor
FROM S068DFA4.IVPDAT.PORCH P
LEFT JOIN IVPDAT.POH POH
    ON P.JSPO# = POH.KAPO#
LEFT JOIN S068DFA4.IVPDAT.STKMP S
    ON TRIM(P."JSPT#") = TRIM(S.AWPART)
LEFT JOIN S068DFA4.IVPDAT.USRC U
    ON S.AWPART = U.MFKEY2
WHERE P."JSPT#" <> ''
  AND P.JSRDAT >= CURRENT_DATE - 365 DAYS
  AND POH.KAOSTS <> 'C'
ORDER BY P.JSRDAT DESC, P.JSPO#
"""


@st.cache_data(show_spinner=False)
def fetch_dashboard_receiving():
    try:
        conn = pyodbc.connect("DSN=IVPDAT;", autocommit=True)
        df = pd.read_sql(SQL_DASHBOARD_RECEIVING, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to load SQL receiving: {e}")
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def fetch_stock_for_articles(articles: tuple[str, ...]) -> pd.DataFrame:
    cols = ["Item", "Article", "Description", "QOH", "Allocation", "QCHold_QCI", "QCHold_QCH", "Variant"]
    if not articles:
        return pd.DataFrame(columns=cols)

    rows: list[dict] = []
    conn = None
    try:
        conn = pyodbc.connect("DSN=IVPDAT;", autocommit=True)
        cur = conn.cursor()
        arts = [str(a).strip() for a in articles if str(a).strip()]
        seen = set()
        unique_arts = []
        for art in arts:
            if art not in seen:
                seen.add(art)
                unique_arts.append(art)

        chunk_size = 200
        primary_rows: dict[str, dict] = {}
        for i in range(0, len(unique_arts), chunk_size):
            batch = unique_arts[i : i + chunk_size]
            placeholders = ",".join(["?"] * len(batch))
            sql = SQL_STOCK_BY_ARTICLE.format(placeholders=placeholders)
            cur.execute(sql, batch)
            for r in cur.fetchall():
                art = "" if r[1] is None else str(r[1]).strip()
                if not art:
                    continue
                desc1 = "" if r[2] is None else str(r[2]).strip()
                primary_rows[art] = {
                    "Item": r[0],
                    "Article": art,
                    "Description": desc1,
                    "QOH": float(r[3] or 0),
                    "Allocation": float(r[4] or 0),
                    "QCHold_QCI": float(r[5] or 0),
                    "QCHold_QCH": float(r[6] or 0),
                    "Variant": float(r[7] or 0),
                }

        missing = [art for art in unique_arts if art not in primary_rows]
        fallback_rows: dict[str, dict] = {}
        for i in range(0, len(missing), chunk_size):
            batch = missing[i : i + chunk_size]
            placeholders = ",".join(["?"] * len(batch))
            sql = SQL_STOCK_BY_ARTICLE_FALLBACK.format(placeholders=placeholders)
            cur.execute(sql, batch)
            for r2 in cur.fetchall():
                art = "" if r2[1] is None else str(r2[1]).strip()
                if not art:
                    continue
                desc1 = "" if r2[2] is None else str(r2[2]).strip()
                desc2 = "" if r2[3] is None else str(r2[3]).strip()
                desc = " ".join([d for d in [desc1, desc2] if d])
                fallback_rows[art] = {
                    "Item": r2[0],
                    "Article": art,
                    "Description": desc,
                    "QOH": float(r2[4] or 0),
                    "Allocation": float(r2[5] or 0),
                    "QCHold_QCI": float(r2[6] or 0),
                    "QCHold_QCH": float(r2[7] or 0),
                    "Variant": float(r2[8] or 0),
                }

        for art in unique_arts:
            rec = primary_rows.get(art) or fallback_rows.get(art)
            if rec:
                rows.append(rec)
            else:
                rows.append(
                    {
                        "Item": None,
                        "Article": art,
                        "Description": None,
                        "QOH": 0.0,
                        "Allocation": 0.0,
                        "QCHold_QCI": 0.0,
                        "QCHold_QCH": 0.0,
                        "Variant": 0.0,
                    }
                )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return pd.DataFrame(rows).drop_duplicates(subset=["Article"])


def build_item_map_for_articles(articles: tuple[str, ...]) -> dict[str, str]:
    """Returns {Article -> PCC Item}. Uses SQL_STOCK_BY_ARTICLE."""
    arts = tuple(_norm_article(a) for a in articles if _norm_article(a))
    if not arts:
        return {}
    df = fetch_stock_for_articles(arts)
    if df is None or df.empty:
        return {}
    out = {}
    for a, it in zip(df["Article"].astype(str), df["Item"]):
        a2 = _norm_article(a)
        it2 = "" if it is None or (isinstance(it, float) and pd.isna(it)) else str(it).strip()
        if a2:
            out[a2] = it2
    return out


# -------------------- DEMAND + ALLOCATION --------------------
def build_component_demands(df_show: pd.DataFrame) -> pd.DataFrame:
    """
    IMPORTANT FIX:
      - If NeedQty is 0 (bad data), we skip it completely so it won't become "Component Supported".
    """
    demand_rows: list[dict] = []
    for _, r in df_show.iterrows():
        po_line = r.get("PO-Line", "")
        dd = r.get("DeliveryDate", None)
        qty_ea = _to_float(r.get("QtyEA", 0.0))

        for b in BUCKETS:
            arts = parse_csv_list(r.get(b, ""))
            qtys = parse_csv_qtys(r.get(f"{b}_Qty", ""))
            per_qtys = parse_csv_qtys(r.get(f"{b}_Per", ""))

            if len(qtys) < len(arts):
                qtys = qtys + [0.0] * (len(arts) - len(qtys))
            if len(per_qtys) < len(arts):
                per_qtys = per_qtys + [0.0] * (len(arts) - len(per_qtys))

            for art, need, per_qty in zip(arts, qtys, per_qtys):
                art = str(art).strip() if art else ""
                needf = round(float(need or 0.0))
                if not art:
                    continue
                if needf <= 1e-9:
                    continue
                demand_rows.append(
                    {
                        "PO-Line": po_line,
                        "DeliveryDate": dd,
                        "Component": b,
                        "ComponentArticle": art,
                        "NeedQty": needf,
                        "QtyPerAssembly": float(per_qty or 0.0),
                        "QtyEA": qty_ea,
                    }
                )
    return pd.DataFrame(demand_rows)


def allocate_by_delivery(demands: pd.DataFrame, stock: pd.DataFrame) -> pd.DataFrame:
    if demands.empty:
        return demands.assign(Status="", Short=0.0, Allocated=0.0, AvailableStart=0.0)

    stock2 = stock.copy()
    stock2["Avail"] = (
        stock2["QOH"].fillna(0)
        - stock2["QCHold_QCI"].fillna(0)
        - stock2["QCHold_QCH"].fillna(0)
    )
    avail_map = {str(a).strip(): float(v) for a, v in zip(stock2["Article"], stock2["Avail"])}

    d = demands.copy()
    d["DeliveryDate_sort"] = pd.to_datetime(d["DeliveryDate"], errors="coerce")
    d = d.sort_values(["DeliveryDate_sort", "PO-Line", "ComponentArticle"], kind="stable")

    remaining = dict(avail_map)
    starts, allocs, shorts = [], [], []

    for _, row in d.iterrows():
        art = str(row["ComponentArticle"]).strip()
        need = float(row["NeedQty"] or 0.0)
        start_avail = float(remaining.get(art, 0.0))
        alloc = min(start_avail, need)
        short = max(0.0, need - alloc)
        remaining[art] = start_avail - alloc

        starts.append(start_avail)
        allocs.append(alloc)
        shorts.append(short)

    d["AvailableStart"] = starts
    d["Allocated"] = allocs
    d["Short"] = shorts
    d["Status"] = d["Short"].apply(lambda x: "OK" if x <= 1e-9 else "SHORT")
    return d.drop(columns=["DeliveryDate_sort"])


# -------------------- REFRESH HELPERS --------------------
def refresh_missing_pdf_entries(folder: str, excel_df: pd.DataFrame) -> None:
    if excel_df is None or excel_df.empty:
        return

    current_map: dict = st.session_state.get("pdf_map", {}) or {}
    current_meta: dict = st.session_state.get("pdf_meta", {}) or {}
    pairs = set(zip(excel_df["PO_norm"].astype(str), excel_df["Line_norm"].astype(str)))
    missing_pairs = [p for p in pairs if p not in current_map]
    missing_pos = sorted(set(po for po, _ln in missing_pairs))
    missing_meta_pos = sorted(set(po for po, _ln in pairs if str(po) not in current_meta))
    if not missing_pairs and not missing_meta_pos:
        return

    idx = build_pdf_index(folder)

    for po in missing_pos:
        pdf_path = idx.get(str(po))
        if not pdf_path:
            continue
        per_line = parse_po_pdf_by_line(Path(pdf_path))
        for line_norm, buckets in per_line.items():
            current_map[(str(po), str(line_norm))] = buckets

    st.session_state["pdf_map"] = current_map

    for po in missing_meta_pos:
        pdf_path = idx.get(str(po))
        if not pdf_path:
            continue
        current_meta[str(po)] = parse_po_pdf_meta(Path(pdf_path))

    st.session_state["pdf_meta"] = current_meta


def refresh_missing_stock_articles(articles: tuple[str, ...]) -> pd.DataFrame:
    cache: pd.DataFrame = st.session_state.get("stock_cache", pd.DataFrame())
    if cache is None or cache.empty:
        missing = articles
    else:
        have = set(cache["Article"].astype(str).str.strip())
        missing = tuple(a for a in articles if str(a).strip() not in have)

    if missing:
        new_df = fetch_stock_for_articles(missing)
        if cache is None or cache.empty:
            cache = new_df.copy()
        else:
            cache = pd.concat([cache, new_df], ignore_index=True).drop_duplicates(
                subset=["Article"], keep="last"
            )

    st.session_state["stock_cache"] = cache
    return cache


def refresh_missing_fg_items(articles: tuple[str, ...]) -> dict[str, str]:
    """Separate lightweight cache for FG Article -> Item (PCC item)."""
    cache: dict = st.session_state.get("fg_item_cache", {}) or {}
    norm_arts = tuple(_norm_article(a) for a in articles if _norm_article(a))
    missing = tuple(a for a in norm_arts if a and a not in cache)

    if missing:
        new_map = build_item_map_for_articles(missing)
        cache.update(new_map)

    st.session_state["fg_item_cache"] = cache
    return cache


# -------------------- SELECTION (CHECKBOX) --------------------
def _open_selected_row_from_editor(df_editor: pd.DataFrame):
    if df_editor is None or df_editor.empty or "Select" not in df_editor.columns:
        return

    checked = df_editor.index[df_editor["Select"] == True].tolist()
    if not checked:
        st.session_state["selected_po"] = ""
        return

    i = checked[0]
    po = str(df_editor.loc[i, "PO-Line"])
    st.session_state["selected_po"] = po
    st.session_state["page"] = "details"
    st.rerun()


# -------------------- SEARCH CLEAR CALLBACK --------------------
def clear_global_search():
    st.session_state["global_search"] = ""
    st.session_state["editor_nonce"] = int(st.session_state.get("editor_nonce", 0) or 0) + 1


def update_page_from_selector():
    label = st.session_state.get("page_selector", "Material Review")
    if label == "FS Requests":
        st.session_state["page"] = "fs_requests"
        st.session_state["selected_po"] = ""
        st.session_state["fs_status_auto_loaded"] = False
    else:
        st.session_state["page"] = "main"
        st.session_state["selected_po"] = ""


# -------------------- APP --------------------
st.set_page_config(page_title="Material Review", layout="wide")

st.session_state.setdefault("merged", load_last())
st.session_state.setdefault("pdf_map", {})
st.session_state.setdefault("pdf_meta", {})
st.session_state.setdefault("stock_cache", pd.DataFrame())
st.session_state.setdefault("fg_item_cache", {})  
st.session_state.setdefault("excel_df", None)
st.session_state.setdefault("skipped_files", [])
st.session_state.setdefault("dismiss_skipped_files", False)
st.session_state.setdefault("uploader_nonce", 0)
st.session_state.setdefault("last_pdf_mtime", 0.0)
st.session_state.setdefault("last_excel_sig", None)

st.session_state.setdefault("alloc_df", pd.DataFrame())
st.session_state.setdefault("stock_df", pd.DataFrame())

st.session_state.setdefault("show_components", False)
st.session_state.setdefault("show_component_qtys", False)
st.session_state.setdefault("show_filled_candle", False)
st.session_state.setdefault("table_expanded", False)
st.session_state.setdefault("fullscreen_requested", False)

st.session_state.setdefault("page", "main")
st.session_state.setdefault("selected_po", "")
st.session_state.setdefault("fs_master_path", FS_MASTER_PATH_DEFAULT)

st.session_state.setdefault("notes_map", load_notes())
st.session_state.setdefault("support_overrides", load_component_support())
st.session_state.setdefault("last_view_snapshot", load_last_view_snapshot())
st.session_state.setdefault("change_history", load_change_history())

st.session_state.setdefault("editor_nonce", 0)
st.session_state.setdefault("fpn_path", FPN_MASTER_PATH_DEFAULT)
st.session_state.setdefault("sched_path", SCHED_REPORT_PATH_DEFAULT)

page = st.session_state.get("page")

if page in ("details", "dashboard"):
    st.markdown(BASE_CSS + SCROLL_DETAILS, unsafe_allow_html=True)
else:
    st.markdown(BASE_CSS + SEARCH_CSS + SCROLL_MAIN, unsafe_allow_html=True)

with st.sidebar:
    sidebar_clock()
    st.divider()
    st.markdown("<div style='margin-top:-0.35rem;'></div>", unsafe_allow_html=True)
    page_options = ["Material Review", "FS Requests"]
    current_page = st.session_state.get("page", "main")
    current_label = "FS Requests" if current_page == "fs_requests" else "Material Review"
    st.radio(
        "Page",
        options=page_options,
        index=page_options.index(current_label),
        key="page_selector",
        on_change=update_page_from_selector,
        horizontal=False,
    )
    if st.session_state.get("page") != "fs_requests":
        excel_files = st.file_uploader(
            "Upload Excel files",
            type=["xls", "xlsx"],
            accept_multiple_files=True,
            key=f"excel_uploader_{st.session_state.get('uploader_nonce', 0)}",
        )
        pdf_folder = st.text_input("PDF folder", value=PDF_FOLDER_DEFAULT)
        run = st.button("Run", type="primary")
    else:
        excel_files = None
        pdf_folder = PDF_FOLDER_DEFAULT
        run = False

if st.session_state.get("page") != "fs_requests":
    st.markdown('<div id="iconbar">', unsafe_allow_html=True)

    t1, t_search, _t_clear_unused, t6, t2, t3, t4, t5 = st.columns(
        [20, 3.2, 0.55, 1, 1, 1, 1, 1],
        gap="small",
        vertical_alignment="center",
    )

    with t_search:
        st.markdown('<div id="corner-search">', unsafe_allow_html=True)
        st.text_input("", key="global_search", placeholder="Search…", label_visibility="collapsed")
        if (st.session_state.get("global_search") or "").strip():
            st.button("✕", key="btn_clear_search", help="Clear search", on_click=clear_global_search)
        st.markdown("</div>", unsafe_allow_html=True)

    with t6:
        if st.button("📊", help="Open Dashboard", key="btn_dashboard"):
            st.session_state["page"] = "dashboard"
            st.rerun()

    with t1:
        title = "Material Review"
        st.markdown(
            f"<h2 style='margin:-0.4rem 0 0 0; padding:0; line-height:1.05;'>{title}</h2>",
            unsafe_allow_html=True,
        )

    with t2:
        refresh_btn = st.button("↻", help="Refresh missing PDF + missing inventory", key="btn_refresh")

    with t3:
        download_slot = st.empty()

    with t4:
        if st.button("⤢", help="Expand/Collapse table", key="btn_expand"):
            st.session_state["table_expanded"] = not st.session_state["table_expanded"]
            if st.session_state["table_expanded"]:
                st.session_state["fullscreen_requested"] = False

    with t5:
        with st.popover("👁", help="Show/Hide columns"):
            st.selectbox(
                "Flag filter",
                options=["All", "🟪 Purple (small short/low avail)", "🟥 Red (short)", "🟩 Green (OK)", "No Flag"],
                key="flag_filter",
            )
            st.checkbox("Show component columns", key="show_components")
            st.checkbox("Show component qty columns", key="show_component_qtys")
            st.checkbox("Show component per-assembly columns", key="show_component_per")
            st.checkbox("Show filled candle", key="show_filled_candle")

    st.markdown("</div>", unsafe_allow_html=True)
else:
    refresh_btn = False
    download_slot = st.empty()

msg_area = st.empty()
bio = None

did_roll_forward = False
roll_forward_ts = None

if st.session_state.get("page") == "fs_requests":
    FS.render_fs_requests_page()
    st.stop()

# -------------------- RUN --------------------
if run:
    st.session_state["dismiss_skipped_files"] = False
    st.session_state["page"] = "main"
    st.session_state["selected_po"] = ""

    if not excel_files:
        st.error("Upload Excel file(s) first.")
        st.session_state["merged"] = None
        st.session_state["excel_df"] = None
        st.session_state["pdf_map"] = {}
        st.session_state["stock_cache"] = pd.DataFrame()
        st.session_state["fg_item_cache"] = {}
        st.session_state["skipped_files"] = []
        st.session_state["alloc_df"] = pd.DataFrame()
        st.session_state["stock_df"] = pd.DataFrame()
        download_slot.empty()
    else:
        download_slot.markdown(
            "<div style='width:42px;height:42px;display:flex;align-items:center;justify-content:center;'>⏳</div>",
            unsafe_allow_html=True,
        )

        status_box = st.empty()
        prog_slot = st.empty()

        try:
            excel_sig = tuple(sorted((f.name, getattr(f, "size", None)) for f in excel_files))
            if excel_sig != st.session_state.get("last_excel_sig"):
                st.cache_data.clear()
                st.session_state["pdf_map"] = {}
                st.session_state["pdf_meta"] = {}
                st.session_state["stock_cache"] = pd.DataFrame()
                st.session_state["fg_item_cache"] = {}
                st.session_state["last_excel_sig"] = excel_sig

            prog = prog_slot.progress(10)
            status_box.info("📊 Combining Excel…")
            with st.spinner("Combining Excel..."):
                excel_df, skipped = combine_excels(excel_files)
            st.session_state["skipped_files"] = skipped

            needed_pos = tuple(sorted(set(excel_df["PO_norm"].astype(str))))
            pdf_mtime = pdf_folder_mtime(pdf_folder)
            if pdf_mtime > float(st.session_state.get("last_pdf_mtime", 0.0)):
                st.session_state["pdf_map"] = {}
                st.session_state["pdf_meta"] = {}
                st.session_state["last_pdf_mtime"] = pdf_mtime
            prog.progress(45)
            status_box.info("📄 Extracting PDF components…")
            with st.spinner("Extracting PDF components..."):
                pdf_map = extract_pdf_map(pdf_folder, needed_pos, pdf_mtime)
                pdf_meta = extract_pdf_meta_map(pdf_folder, needed_pos, pdf_mtime)

            prog.progress(75)
            status_box.info("🔗 Merging…")
            merged_df = merge(excel_df, pdf_map, pdf_meta)

            prog.progress(95)
            status_box.info("💾 Saving…")
            persist_last(merged_df)

            st.session_state["excel_df"] = excel_df.copy()
            st.session_state["pdf_map"] = pdf_map.copy()
            st.session_state["pdf_meta"] = pdf_meta.copy()
            st.session_state["stock_cache"] = pd.DataFrame()
            st.session_state["fg_item_cache"] = {}
            st.session_state["merged"] = merged_df
            st.session_state["alloc_df"] = pd.DataFrame()
            st.session_state["stock_df"] = pd.DataFrame()
            st.session_state["uploader_nonce"] = int(st.session_state.get("uploader_nonce", 0)) + 1

            did_roll_forward = True
            roll_forward_ts = datetime.now()

            st.toast("Done", icon="✅")

        except Exception as e:
            st.session_state["merged"] = None
            st.error(f"Failed: {e}")
            download_slot.empty()
        finally:
            status_box.empty()
            prog_slot.empty()

# -------------------- REFRESH --------------------
if refresh_btn and st.session_state.get("merged") is not None:
    download_slot.markdown(
        "<div style='width:42px;height:42px;display:flex;align-items:center;justify-content:center;'>⏳</div>",
        unsafe_allow_html=True,
    )

    status_box = st.empty()
    prog_slot = st.empty()

    try:
        base_excel = st.session_state.get("excel_df")
        if base_excel is None or getattr(base_excel, "empty", True):
            base_excel = st.session_state["merged"][["PO_norm", "Line_norm"]].drop_duplicates()

        prog = prog_slot.progress(35)
        status_box.info("📄 Refreshing missing PDF…")
        refresh_missing_pdf_entries(pdf_folder, base_excel)

        prog.progress(75)
        status_box.info("🔗 Refreshing view…")
        src = st.session_state.get("excel_df")
        if src is None or getattr(src, "empty", True):
            src = st.session_state["merged"].copy()
        st.session_state["merged"] = merge(src, st.session_state["pdf_map"], st.session_state.get("pdf_meta", {}))

        st.session_state["alloc_df"] = pd.DataFrame()
        st.session_state["stock_df"] = pd.DataFrame()

        did_roll_forward = True
        roll_forward_ts = datetime.now()

        st.toast("Refreshed", icon="✅")
    finally:
        status_box.empty()
        prog_slot.empty()

# -------------------- COMPUTE VIEW + EXPORT PREP --------------------
if st.session_state.get("merged") is None:
    st.caption("Upload Excel(s) on the left and click Run.")
else:
    if not did_roll_forward and st.session_state.get("df_show_cached") is not None:
        df_show = st.session_state["df_show_cached"]
    else:
        df_show = st.session_state["merged"].copy()
        df_show["HasComponents"] = df_show.apply(row_has_components, axis=1)

        # NEW: Add PCC Item for finished good Article
        fg_articles = tuple(sorted(set(df_show["Article"].astype(str).map(_norm_article))))
        fg_item_map = refresh_missing_fg_items(fg_articles)
        df_show["Item"] = df_show["Article"].astype(str).map(lambda x: fg_item_map.get(_norm_article(x), "")).fillna("")

        demands = build_component_demands(df_show[df_show["HasComponents"]].copy())

        if demands.empty:
            df_show["Status"] = ""
            df_show["Flag"] = ""
            df_show["Short_Detail"] = ""
            st.session_state["alloc_df"] = pd.DataFrame()
            st.session_state["stock_df"] = pd.DataFrame()
        else:
            comp_articles = tuple(sorted(set(demands["ComponentArticle"].astype(str).str.strip())))
            stock_df = refresh_missing_stock_articles(comp_articles)
            alloc = allocate_by_delivery(demands, stock_df)

            st.session_state["alloc_df"] = alloc.copy()
            st.session_state["stock_df"] = stock_df.copy()

            has_any_short = (alloc["Status"] == "SHORT").any()
            incoming_map, _incoming_file_label = (get_incoming_context() if has_any_short else ({}, ""))

            short_by_po = (
                alloc.groupby("PO-Line")["Status"]
                .apply(lambda s: "SHORT" if (s == "SHORT").any() else "OK")
                .to_dict()
            )

            low_avail_pos = set(
                alloc.loc[alloc["AvailableStart"].fillna(0).astype(float) < 100, "PO-Line"]
                .astype(str)
                .unique()
                .tolist()
            )

            alloc_with_qc = alloc.merge(
                stock_df[["Article", "QCHold_QCI", "QCHold_QCH"]],
                left_on="ComponentArticle",
                right_on="Article",
                how="left",
            )

            def _detail_with_incoming(g: pd.DataFrame) -> str:
                parts = []
                for c, a, n, v, s, qci, qch in zip(
                    g["Component"],
                    g["ComponentArticle"],
                    g["NeedQty"],
                    g["AvailableStart"],
                    g["Short"],
                    g["QCHold_QCI"],
                    g["QCHold_QCH"],
                ):
                    unit = "kg" if str(c).strip().upper() == "FRG" else "pcs"
                    inc_txt = incoming_text_for_article(a, incoming_map, needed_qty=s)
                    pieces = [f"{c} {a} (need {n:.0f} {unit}, avail {v:.0f} {unit})"]
                    if inc_txt:
                        pieces.append(inc_txt)
                    qci_val = float(qci or 0.0)
                    qch_val = float(qch or 0.0)
                    if qci_val > 0 or qch_val > 0:
                        pieces.append(f"QC Hold QCI {qci_val:.0f}, QCH {qch_val:.0f}")
                    parts.append(" | ".join(pieces))
                return ", ".join(parts)

            detail = (
                alloc_with_qc[alloc_with_qc["Status"] == "SHORT"]
                .groupby("PO-Line")
                .apply(_detail_with_incoming)
                .to_dict()
            )

            df_show["Status"] = df_show["PO-Line"].map(short_by_po).fillna("")
            df_show["Short_Detail"] = df_show["PO-Line"].map(detail).fillna("")

            supported_pos = set(alloc["PO-Line"].astype(str).unique())
            df_show.loc[
                (df_show["Status"] == "OK") & (df_show["PO-Line"].astype(str).isin(supported_pos)),
                "Short_Detail"
            ] = "Component Supported"

            df_show.loc[~df_show["HasComponents"], ["Status", "Short_Detail"]] = ["", ""]

            short_by_po_qty = alloc.groupby("PO-Line")["Short"].sum().to_dict()
            small_short_pos = {
                po
                for po, short in short_by_po_qty.items()
                if float(short or 0.0) > 0 and float(short or 0.0) <= 100
            }

            def _flag_for_row(row) -> str:
                po_key = str(row.get("PO-Line", "")).strip()
                if po_key in small_short_pos:
                    return "🟪"
                if row.get("Status") == "SHORT":
                    return "🟥"
                if po_key in low_avail_pos:
                    return "🟪"
                if row.get("Status") == "OK":
                    return "🟩"
                return ""

            df_show["Flag"] = df_show.apply(_flag_for_row, axis=1)

        support_overrides = st.session_state.get("support_overrides", {}) or {}
        df_show = apply_component_support_overrides(df_show, support_overrides)

        fs_maps = load_fs_master(st.session_state.get("fs_master_path", FS_MASTER_PATH_DEFAULT))
        fs_info = df_show.apply(lambda r: fs_lookup_for_row(r, fs_maps), axis=1)
        df_show["FS_Lot"] = fs_info.apply(lambda d: d.get("FS_Lot", "") if isinstance(d, dict) else "")
        df_show["FS_Status"] = fs_info.apply(lambda d: d.get("FS_Status", "") if isinstance(d, dict) else "")
        df_show["FS_Info"] = df_show.apply(
            lambda r: format_fs_info(r.get("FS_Lot", ""), r.get("FS_Status", "")),
            axis=1,
        )

        df_show["Short_Detail"] = df_show.apply(
            lambda r: (
                f"{r.get('PO_Notes', '').strip()}"
                if r.get("PO_Notes", "").strip() and not str(r.get("Short_Detail", "")).strip()
                else (
                    f"{r.get('PO_Notes', '').strip()} | {r.get('Short_Detail', '').strip()}"
                    if r.get("PO_Notes", "").strip() and str(r.get("Short_Detail", "")).strip()
                    else r.get("Short_Detail", "")
                )
            ),
            axis=1,
        )

        notes_map = st.session_state.get("notes_map", {}) or {}
        df_show["Notes"] = (
            df_show["PO-Line"]
            .astype(str)
            .map(lambda k: notes_map.get(str(k).strip(), ""))
            .fillna("")
        )

        df_show["DeliveryDate"] = df_show["DeliveryDate"].map(_format_date_only)
        df_show["StatisticalDate"] = df_show["StatisticalDate"].map(_format_date_only)

        filled_vals = df_show.get("Filled_Candle", pd.Series(dtype=str)).astype(str).str.strip()
        fpn_df = load_fpn_lookup(st.session_state.get("fpn_path", FPN_MASTER_PATH_DEFAULT))
        sched_maps = load_sched_lookup(st.session_state.get("sched_path", SCHED_REPORT_PATH_DEFAULT))

        fpn_map = dict(zip(fpn_df["FilledCandle"].astype(str), fpn_df["ResultK"]))
        df_show["Lookup_K"] = filled_vals.map(lambda x: fpn_map.get(x, "") if x else "")

        def _lookup_sched(val):
            if not val or (isinstance(val, float) and pd.isna(val)):
                return ""
            for sheet_name in (SCHED_SHEET_2025, SCHED_SHEET_ARCHIVE):
                df = sched_maps.get(sheet_name)
                if df is None or df.empty:
                    continue
                hit = _xlookup_prev(val, df["LookupI"], df["ReturnA"])
                if hit != "" and not (isinstance(hit, float) and pd.isna(hit)):
                    return hit
            return ""

        df_show["Lookup_L"] = df_show["Lookup_K"].map(_lookup_sched)
        df_show["Older_Than_2Y"] = df_show["Lookup_L"].map(
            lambda x: "Y" if _format_date_only(x) and pd.to_datetime(x, errors="coerce") <
            (pd.Timestamp.today().normalize() - pd.DateOffset(years=2)) else "N"
        )
        df_show["Burn"] = df_show.apply(
            lambda r: "" if not str(r.get("Filled_Candle", "")).strip() else (
                "Y" if (not str(r.get("Lookup_K", "")).strip()
                        or (str(r.get("Lookup_K", "")).strip() and str(r.get("Older_Than_2Y", "")).strip() == "Y"))
                else "N"
            ),
            axis=1,
        )
        df_show["Burn"] = df_show["Burn"].map(lambda x: "Y" if str(x).strip() == "Y" else "")

    skipped_files = st.session_state.get("skipped_files", [])
    if skipped_files and not st.session_state.get("dismiss_skipped_files", False):
        msg = "Some files were skipped:\n" + "\n".join([f"- {n}: {r}" for n, r in skipped_files])
        with msg_area:
            dismissible_notice("skipped_files", msg, kind="warning")
    else:
        msg_area.empty()

    if did_roll_forward and roll_forward_ts is not None:
        prev_view = st.session_state.get("last_view_snapshot")
        cur_view = df_show[[c for c in CHANGE_VIEW_COLS if c in df_show.columns]].copy()

        df_log = compute_change_log(cur_view, prev_view, roll_forward_ts)
        append_change_history(df_log)

        st.session_state["change_history"] = load_change_history()

        persist_view_snapshot(cur_view)
        st.session_state["last_view_snapshot"] = load_last_view_snapshot()

    st.session_state["df_show_cached"] = df_show.copy()

  
    export_cols = [c for c in BASE_COLS if c in df_show.columns]
    if "Burn" in df_show.columns:
        export_cols.append("Burn")
    if "FS_Info" in df_show.columns:
        export_cols.append("FS_Info")
    export_cols += ["Short_Detail", "Notes"]
    df_export = df_show[export_cols].copy()
    df_export = add_serial(df_export)

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Sheet1")

if bio is not None:
    download_slot.download_button(
        "⤓",
        help="Download XLSX",
        data=bio.getvalue(),
        file_name="material_review.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_xlsx_top",
    )
else:
    download_slot.empty()

# -------------------- PAGES --------------------
if st.session_state.get("merged") is None:
    st.stop()


def render_dashboard_page(df_show: pd.DataFrame):
    today = datetime.today().date()
    next_7 = today + pd.Timedelta(days=7)

    top = st.container()
    c1, c2 = top.columns([1, 8], vertical_alignment="center")
    with c1:
        if st.button("← Back", use_container_width=True):
            st.session_state["page"] = "main"
            st.rerun()
    with c2:
        st.markdown("## 📊 Planner Dashboard")

    st.markdown("### 📅 POs Due Soon")
    due_df = df_show.copy()
    due_df["DeliveryDate_dt"] = pd.to_datetime(due_df["DeliveryDate"], errors="coerce").dt.date

    # Ensure Item exists (PCC item)
    if "Item" not in due_df.columns:
        due_df["Item"] = ""
    else:
        due_df["Item"] = due_df["Item"].fillna("")

    due_today = due_df[due_df["DeliveryDate_dt"] == today].copy()
    due_7d = due_df[(due_df["DeliveryDate_dt"] > today) & (due_df["DeliveryDate_dt"] <= next_7)].copy()

    st.metric("Due Today", len(due_today))
    st.metric("Due Next 7 Days", len(due_7d))

    if not due_today.empty:
        st.markdown("**POs Due Today:**")
        cols = [
            c
            for c in ["PO-Line", "Item", "Article", "Description", "DeliveryDate", "Status"]
            if c in due_today.columns
        ]
        df_tbl = add_serial(due_today[cols].copy())
        st.dataframe(df_tbl, use_container_width=True, hide_index=True)

    if not due_7d.empty:
        st.markdown("**POs Due Next 7 Days:**")
        cols = [
            c
            for c in ["PO-Line", "Item", "Article", "Description", "DeliveryDate", "Status"]
            if c in due_7d.columns
        ]
        df_tbl = add_serial(due_7d[cols].copy())
        st.dataframe(df_tbl, use_container_width=True, hide_index=True)

    incoming_header, incoming_picker = st.columns([4, 1.4], vertical_alignment="center")
    with incoming_header:
        st.markdown("### 📦 Incoming Schedule")
    with incoming_picker:
        incoming_date = st.date_input("Incoming date", value=today, key="incoming_date")
    incoming_map, _ = get_incoming_context()
    expected_today = []
    for art, shipments in incoming_map.items():
        for shipment in shipments:
            updates = str(shipment.get("updates", "") or "")
            eta = str(shipment.get("eta", "") or "")
            if _incoming_matches_date(updates, eta, incoming_date):
                expected_today.append((art, shipment))

    if expected_today:
        incoming_articles = tuple({art for art, _ in expected_today})
        stock_df = refresh_missing_stock_articles(incoming_articles)
        desc_map = {
            str(a).strip(): str(d).strip()
            for a, d in zip(stock_df["Article"].astype(str), stock_df["Description"].astype(str))
        }
        rows = []
        for art, shipment in expected_today:
            # Find which POs use this article in any component columns (best-effort)
            used_in = df_show[
                df_show.astype(str).apply(lambda r: art in " | ".join(r.values), axis=1)
            ]["PO-Line"].astype(str).unique().tolist()

            # PCC Item for this incoming Article
            item_map = refresh_missing_fg_items((art,))
            rows.append(
                {
                    "Item": item_map.get(_norm_article(art), ""),
                    "Article": art,
                    "Description": desc_map.get(str(art).strip(), ""),
                    "Qty": float(shipment.get("qty", 0.0) or 0.0),
                    "Used In": ", ".join(used_in),
                    "Updates": shipment.get("updates", ""),
                    "ETA": shipment.get("eta", ""),
                }
            )
        df_in = pd.DataFrame(rows)
        df_in = add_serial(df_in)
        df_in_style = df_in.style.apply(
            lambda _row: ["background-color: #dcfce7"] * len(df_in.columns),
            axis=1,
        )
        st.dataframe(df_in_style, use_container_width=True, hide_index=True)
    else:
        st.caption("No incoming components found for today.")

    st.markdown("### 📥 Today's Receiving")
    sql_receiving = fetch_dashboard_receiving()
    if sql_receiving.empty:
        st.caption("No receiving records found.")
    else:
        sql_receiving = sql_receiving.copy()
        if "ReceiptDate" not in sql_receiving.columns:
            lower_cols = {str(c).strip().lower(): c for c in sql_receiving.columns}
            fallback = None
            for candidate in ("receiptdate", "po_date", "podate", "jsrdat"):
                if candidate in lower_cols:
                    fallback = lower_cols[candidate]
                    break
            if fallback:
                sql_receiving = sql_receiving.rename(columns={fallback: "ReceiptDate"})
        sql_receiving["ReceiptDate"] = pd.to_datetime(sql_receiving["ReceiptDate"], errors="coerce").dt.date

        sql_today = sql_receiving[sql_receiving["ReceiptDate"] == today].copy()
        sql_7d = sql_receiving[
            (sql_receiving["ReceiptDate"] > today) & (sql_receiving["ReceiptDate"] <= next_7)
        ].copy()

        if not sql_today.empty:
            st.markdown("**Received Today:**")
            st.dataframe(add_serial(sql_today), use_container_width=True, hide_index=True)

        if not sql_7d.empty:
            st.markdown("**Expected Receiving Next 7 Days:**")
            st.dataframe(add_serial(sql_7d), use_container_width=True, hide_index=True)


def render_change_panel():
    hist: pd.DataFrame = st.session_state.get("change_history", pd.DataFrame())
    if hist is None or hist.empty:
        with st.expander("🕒 Changes (since Run/Refresh history)", expanded=False):
            st.caption("No saved changes yet. Changes will appear after the next Run or Refresh.")
        return

    df = hist.copy()
    df["RunTS"] = pd.to_datetime(df["RunTS"], errors="coerce")
    df = df.dropna(subset=["RunTS"]).copy()
    df["RunDate"] = df["RunTS"].dt.date

    min_d = df["RunDate"].min()
    max_d = df["RunDate"].max()

    with st.expander("🕒 Changes (since Run/Refresh history)", expanded=False):
        c1, c2, c3, c4 = st.columns([1.1, 1.1, 1.2, 1.2], vertical_alignment="center")

        with c1:
            d_from = st.date_input("From", value=min_d, min_value=min_d, max_value=max_d, key="chg_from")
        with c2:
            d_to = st.date_input("To", value=max_d, min_value=min_d, max_value=max_d, key="chg_to")
        with c3:
            st.text_input("Search", key="change_search", placeholder="Search PO, field, or value")
        with c4:
            if st.button("Clear history", use_container_width=True):
                try:
                    if CHANGE_LOG_FILE.exists():
                        CHANGE_LOG_FILE.unlink()
                except Exception:
                    pass
                st.session_state["change_history"] = pd.DataFrame(
                    columns=["RunTS", "PO-Line", "Field", "Old", "New", "Description"]
                )
                st.toast("Change history cleared", icon="🧹")
                st.rerun()

        if d_from and d_to and d_from > d_to:
            st.error("From date must be <= To date.")
            return

        mask = (df["RunDate"] >= d_from) & (df["RunDate"] <= d_to)
        f = df[mask].copy()

        search = (st.session_state.get("change_search") or "").strip().lower()
        if search:
            search_cols = [c for c in f.columns if c != "RunDate"]
            blob = (
                f[search_cols]
                .astype(str)
                .fillna("")
                .agg(" | ".join, axis=1)
                .str.lower()
            )
            f = f[blob.str.contains(re.escape(search), na=False, regex=True)].copy()

        show_cols = [c for c in ["RunTS", "PO-Line", "Description", "Field", "Old", "New"] if c in f.columns]
        f = f[show_cols].sort_values(["RunTS", "PO-Line", "Field"], kind="stable")

        st.dataframe(f, use_container_width=True, hide_index=True, height=320)

        if not f.empty:
            bio2 = changes_to_excel_bytes(f)
            st.download_button(
                "Download changes (XLSX)",
                data=bio2.getvalue(),
                file_name=f"material_review_changes_{d_from}_to_{d_to}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_changes_xlsx",
            )
        else:
            st.caption("No changes found in the selected date range.")


def render_main_page(df_show: pd.DataFrame):
    render_change_panel()

    visible = [c for c in BASE_COLS if c in df_show.columns and c != "Item"]

    if st.session_state["show_components"]:
        visible += [b for b in BUCKETS if b in df_show.columns]
    if st.session_state["show_component_qtys"]:
        visible += [f"{b}_Qty" for b in BUCKETS if f"{b}_Qty" in df_show.columns]
    if st.session_state.get("show_component_per"):
        visible += [f"{b}_Per" for b in BUCKETS if f"{b}_Per" in df_show.columns]
    if st.session_state["show_filled_candle"] and "Filled_Candle" in df_show.columns:
        visible.append("Filled_Candle")

    for c in ["Flag", "Short_Detail", "FS_Info", "Notes"]:
        if c in df_show.columns and c not in visible:
            visible.append(c)

    if "Burn" in df_show.columns:
        flag_idx = visible.index("Flag") if "Flag" in visible else len(visible)
        visible.insert(flag_idx, "Burn")

    view_df = df_show[visible].copy()
    if "Select" not in view_df.columns:
        view_df.insert(0, "Select", False)

    q = (st.session_state.get("global_search") or "").strip()
    if q:
        tokens = [t for t in re.split(r"\s+", q) if t]

        search_cols = [c for c in df_show.columns if c != "Select"]
        blob = (
            df_show[search_cols]
            .astype(str)
            .fillna("")
            .agg(" | ".join, axis=1)
            .str.lower()
        )

        mask = pd.Series(True, index=df_show.index)
        for t in tokens:
            t = re.escape(t.lower())
            mask &= blob.str.contains(t, na=False, regex=True)

        view_df = view_df.loc[mask].copy()

    flag_filter = st.session_state.get("flag_filter", "All")
    if flag_filter != "All":
        if flag_filter == "No Flag":
            view_df = view_df[view_df["Flag"].astype(str).str.strip() == ""].copy()
        else:
            flag_symbol = flag_filter.split(" ")[0]
            view_df = view_df[view_df["Flag"].astype(str).str.strip() == flag_symbol].copy()

    table_h = 920 if st.session_state["table_expanded"] else 720

    if st.session_state.get("table_expanded") and not st.session_state.get("fullscreen_requested"):
        components.html(
            """
            <script>
              const doc = document.documentElement;
              if (doc.requestFullscreen) {
                doc.requestFullscreen().catch(() => {});
              }
            </script>
            """,
            height=0,
        )
        st.session_state["fullscreen_requested"] = True

    if st.session_state.get("table_expanded"):
        column_config = {
            "Select": st.column_config.CheckboxColumn("Select", help="Check to open details", default=False),
            "Notes": st.column_config.TextColumn("Notes", help="Planner notes (saved locally)"),
            "Short_Detail": st.column_config.TextColumn("Short_Detail", max_chars=60),
            "FS_Info": st.column_config.TextColumn("FS_Info", max_chars=60),
            "Description": st.column_config.TextColumn("Description"),
            "PO-Line": st.column_config.TextColumn("PO-Line"),
            "Item": st.column_config.TextColumn("Item", help="PCC Item"),
            "Burn": st.column_config.TextColumn("Burn", help="Filled candle status"),
        }
    else:
        column_config = {
            "Select": st.column_config.CheckboxColumn("Select", help="Check to open details", default=False, width="small"),
            "Notes": st.column_config.TextColumn("Notes", help="Planner notes (saved locally)", width="large"),
            "Short_Detail": st.column_config.TextColumn("Short_Detail", width="large", max_chars=60),
            "FS_Info": st.column_config.TextColumn("FS_Info", width="large", max_chars=60),
            "Description": st.column_config.TextColumn("Description", width="large"),
            "PO-Line": st.column_config.TextColumn("PO-Line", width="smaller"),
            "Item": st.column_config.TextColumn("Item", help="PCC Item", width="small"),
            "Burn": st.column_config.TextColumn("Burn", help="Filled candle status", width="small"),
        }

    if "Burn" in view_df.columns:
        burn_idx = view_df.columns.get_loc("Burn") + 1
        st.markdown(
            f"""
            <style>
            [data-testid="stDataFrame"] div[role="grid"] div[aria-colindex="{burn_idx}"] {{
              color: #dc2626 !important;
              font-weight: 700 !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )

    disabled_cols = [c for c in view_df.columns if c not in ("Select", "Notes")]
    editor_key = f"main_editor_{int(st.session_state.get('editor_nonce', 0) or 0)}"

    edited = st.data_editor(
        view_df,
        use_container_width=True,
        hide_index=True,
        height=table_h,
        column_config=column_config,
        disabled=disabled_cols,
        key=editor_key,
    )

    if edited is not None and not edited.empty and "Notes" in edited.columns and "PO-Line" in edited.columns:
        notes_map = st.session_state.get("notes_map", {}) or {}
        changed_any = False
        for po, note in zip(edited["PO-Line"].astype(str), edited["Notes"].astype(str)):
            po = po.strip()
            if not po:
                continue
            note2 = "" if note is None else str(note)
            if notes_map.get(po, "") != note2:
                notes_map[po] = note2
                changed_any = True
        if changed_any:
            st.session_state["notes_map"] = notes_map
            save_notes(notes_map)
            if st.session_state.get("df_show_cached") is not None:
                st.session_state["df_show_cached"]["Notes"] = (
                    st.session_state["df_show_cached"]["PO-Line"]
                    .astype(str)
                    .map(lambda k: notes_map.get(str(k).strip(), ""))
                    .fillna("")
                )

    _open_selected_row_from_editor(edited)


def render_details_page(df_show: pd.DataFrame):
    selected_po = (st.session_state.get("selected_po") or "").strip()
    if not selected_po:
        st.warning("No PO-Line selected. Go back and check a row.")
        if st.button("← Back"):
            st.session_state["page"] = "main"
            st.rerun()
        return

    top = st.container()
    c1, c2 = top.columns([1, 8], vertical_alignment="center")
    with c1:
        if st.button("← Back", use_container_width=True):
            st.session_state["page"] = "main"
            st.session_state["selected_po"] = ""
            st.rerun()
    with c2:
        st.markdown(f"### PO Details — **{selected_po}**")

    row_df = df_show[df_show["PO-Line"].astype(str) == selected_po]
    if row_df.empty:
        st.error("Selected PO-Line not found in current dataset.")
        return

    row = row_df.iloc[0]
    notes_map = st.session_state.get("notes_map", {}) or {}
    current_note = notes_map.get(selected_po, "")
    support_overrides = st.session_state.get("support_overrides", {}) or {}
    support_checked = bool(support_overrides.get(str(selected_po).strip(), False))
    is_purple_flag = str(row.get("Flag", "")).strip() == "🟪"
    if support_checked and not is_purple_flag:
        support_overrides[str(selected_po).strip()] = False
        st.session_state["support_overrides"] = support_overrides
        save_component_support(support_overrides)
        support_checked = False

    detail_rows = [
        ("Item - Article - Description", f"{row.get('Item','')} - {row.get('Article','')} - {row.get('Description','')}"),
        ("QtyEA", row.get("QtyEA", "")),
        ("DeliveryDate", row.get("DeliveryDate", "")),
        ("StatisticalDate", row.get("StatisticalDate", "")),
        ("Flag", row.get("Flag", "")),
        ("Short Detail", row.get("Short_Detail", "")),
        ("FS Info", row.get("FS_Info", "")),
    ]

    with st.container():
        st.markdown('<div id="po-detail-card"></div>', unsafe_allow_html=True)
        st.markdown(
            f"<div class='po-card-title'>{html.escape(_sanitize_text(selected_po))}</div>",
            unsafe_allow_html=True,
        )
        for label, value in detail_rows:
            safe_label = html.escape(label)
            safe_value = html.escape(_sanitize_text(value))
            st.markdown(
                f"<div class='po-card-row'><div class='po-card-label'>{safe_label}</div>"
                f"<div class='po-card-value'>{safe_value}</div></div>",
                unsafe_allow_html=True,
            )

        override_val = st.checkbox(
            "Component Supported",
            value=support_checked,
            key=f"support_{selected_po}",
            disabled=not is_purple_flag,
        )
        if override_val != support_checked and is_purple_flag:
            support_overrides[str(selected_po).strip()] = override_val
            st.session_state["support_overrides"] = support_overrides
            save_component_support(support_overrides)
            if st.session_state.get("df_show_cached") is not None:
                st.session_state["df_show_cached"] = apply_component_support_overrides(
                    st.session_state["df_show_cached"], support_overrides
                )
            st.rerun()

        st.markdown("<div class='po-card-notes-label'>Planner Notes</div>", unsafe_allow_html=True)
        note_val = st.text_area(
            "Planner Notes",
            value=current_note,
            height=9,
            placeholder="Type notes for this PO-Line…",
            label_visibility="collapsed",
        )
        if note_val != current_note:
            notes_map[selected_po] = note_val
            st.session_state["notes_map"] = notes_map
            save_notes(notes_map)
            if st.session_state.get("df_show_cached") is not None:
                st.session_state["df_show_cached"]["Notes"] = (
                    st.session_state["df_show_cached"]["PO-Line"]
                    .astype(str)
                    .map(lambda k: notes_map.get(str(k).strip(), ""))
                    .fillna("")
                )
            current_note = note_val

    alloc_df: pd.DataFrame = st.session_state.get("alloc_df", pd.DataFrame())
    stock_df: pd.DataFrame = st.session_state.get("stock_df", pd.DataFrame())

    if alloc_df is None or alloc_df.empty:
        st.info("No allocation data (components may be missing for this PO).")
        return

    po_alloc = alloc_df[alloc_df["PO-Line"].astype(str) == selected_po].copy()
    if po_alloc.empty:
        st.info("No component lines found for this PO (or NeedQty was 0 and skipped).")
        return

    stock2 = stock_df.copy()
    stock2["AVAILABLE"] = (
        stock2["QOH"].fillna(0)
        - stock2["QCHold_QCI"].fillna(0)
        - stock2["QCHold_QCH"].fillna(0)
    )

    det = po_alloc.merge(
        stock2,
        left_on="ComponentArticle",
        right_on="Article",
        how="left",
        suffixes=("", "_stk"),
    )

    row_qty_ea = _to_float(row.get("QtyEA", 0.0))
    per_map: dict[str, float] = {}
    for b in BUCKETS:
        arts = parse_csv_list(row.get(b, ""))
        per_vals = parse_csv_qtys(row.get(f"{b}_Per", ""))
        if len(per_vals) < len(arts):
            per_vals = per_vals + [0.0] * (len(arts) - len(per_vals))
        for art, per_val in zip(arts, per_vals):
            art = str(art).strip()
            if not art:
                continue
            if per_val is None:
                continue
            per_map[art] = float(per_val or 0.0)

    if "QtyEA" not in det.columns:
        det["QtyEA"] = row_qty_ea
    else:
        det["QtyEA"] = det["QtyEA"].fillna(row_qty_ea)
    if "QtyPerAssembly" not in det.columns:
        det["QtyPerAssembly"] = det["ComponentArticle"].astype(str).map(per_map).fillna(0.0)
    else:
        det["QtyPerAssembly"] = det["QtyPerAssembly"].fillna(
            det["ComponentArticle"].astype(str).map(per_map).fillna(0.0)
        )

    has_short_here = (det["Status"].astype(str) == "SHORT").any()
    incoming_map, _incoming_file_label = (get_incoming_context() if has_short_here else ({}, ""))

    incoming_col = []
    for a, s, short in zip(det["ComponentArticle"], det["Status"], det["Short"]):
        if str(s) != "SHORT":
            incoming_col.append("")
        else:
            incoming_col.append(incoming_text_for_article(a, incoming_map, needed_qty=short))
    det["INCOMING"] = incoming_col

    quick = pd.DataFrame(
        {
            "TYPE": det["Component"].astype(str),
            "ITEM": det["Item"],
            "ARTICLE": det["ComponentArticle"].astype(str),
            "DESCRIPTION": det["Description"],
            "QOH": det["QOH"].fillna(0).astype(float),
            "QCHOLD_QCI": det["QCHold_QCI"].fillna(0).astype(float),
            "QCHOLD_QCH": det["QCHold_QCH"].fillna(0).astype(float),
            "AVAILABLE": det["AVAILABLE"].fillna(0).astype(float),
            "INCOMING": det["INCOMING"].astype(str),
            "REMAIN_BEFORE_PO": det["AvailableStart"].fillna(0).astype(float),
            "NEED": det["NeedQty"].fillna(0).astype(float),
            "ALLOCATED_TO_PO": det["Allocated"].fillna(0).astype(float),
            "SHORT": det["Short"].fillna(0).astype(float),
            "STATUS": det["Status"].astype(str),
        }
    )

    st.markdown("#### Components (quick review)")
    st.dataframe(add_serial(quick), use_container_width=True, hide_index=True, height=380)

    st.markdown("#### Shared usage (same components)")
    st.caption("FIFO view by DeliveryDate. Shows other PO-Lines consuming the same component articles.")

    _map_df = det[["ComponentArticle", "Item"]].copy()
    _map_df["ComponentArticle"] = _map_df["ComponentArticle"].astype(str).str.strip()
    _map_df = _map_df.drop_duplicates(subset=["ComponentArticle"], keep="first")

    item_map = {
        str(r["ComponentArticle"]).strip(): ("" if pd.isna(r["Item"]) else str(r["Item"]).strip())
        for _, r in _map_df.iterrows()
    }

    arts = _map_df["ComponentArticle"].tolist()

    for art in arts:
        art = str(art).strip()
        if not art:
            continue

        item = item_map.get(art, "")
        title = f"Article {art} ({item})" if item else f"Article {art}"
        st.markdown(f"**{title}**")

        art_alloc = alloc_df[alloc_df["ComponentArticle"].astype(str).str.strip() == art].copy()
        if art_alloc.empty:
            st.caption("No FIFO rows for this article.")
            continue

        art_alloc["DeliveryDate_sort"] = pd.to_datetime(art_alloc["DeliveryDate"], errors="coerce")
        art_alloc = art_alloc.sort_values(["DeliveryDate_sort", "PO-Line"], kind="stable").drop(
            columns=["DeliveryDate_sort"]
        )

        st.dataframe(
            add_serial(
                art_alloc[
                    ["PO-Line", "DeliveryDate", "Component", "NeedQty", "AvailableStart", "Allocated", "Short", "Status"]
                ].copy()
            ),
            use_container_width=True,
            hide_index=True,
            height=220,
        )


if st.session_state.get("page") == "details":
    render_details_page(df_show)
elif st.session_state.get("page") == "dashboard":
    render_dashboard_page(df_show)
elif st.session_state.get("page") == "fs_requests":
    FS.render_fs_requests_page()
else:
    render_main_page(df_show)
