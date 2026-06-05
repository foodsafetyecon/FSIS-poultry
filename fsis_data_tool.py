"""
FSIS Raw Poultry Sampling pipeline (DuckDB, in-memory ZIP):
1) Discover yearly ZIP links on FSIS (FY2014–FY2025)
2) Download ZIPs into memory (BytesIO) — no ZIP files written to disk
3) Extract JSON/CSV content directly from in-memory archive and combine
4) Insert combined data into DuckDB (fsis.db): raw + cleaned tables
5) Export cleaned DataFrame to CSV (and Excel for convenience)

Source page (links discovered automatically):
https://www.fsis.usda.gov/news-events/publications/raw-poultry-sampling
"""

import io
import re
import json
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
import pandas as pd
import duckdb




# ----------------------------
# Configuration
# ----------------------------
BASE_DIR = Path("./data")  # GitHub-safe, relative output folder
FSIS_PAGE_URL = "https://www.fsis.usda.gov/news-events/publications/raw-poultry-sampling"
YEARS = range(2014, 2026)  # FY2014–FY2025 inclusive

# DuckDB database & table names
DB_PATH = BASE_DIR / "fsis.db"
RAW_TABLE = "raw_poultry_primary"
CLEAN_TABLE = "raw_poultry_primary_clean"

# Combined Excel/CSV outputs
OUT_XLSX = BASE_DIR / "combined_primary_table_data_2014_2025.xlsx"
OUT_CSV = BASE_DIR / "cleaned_df.csv"


# ----------------------------
# Utilities
# ----------------------------
def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _get_fsis_zip_links(page_url: str) -> Dict[int, str]:
    """
    Scrape the FSIS Raw Poultry Sampling page for direct .zip links.
    Returns {year: url}, e.g., {2014: "...fy2014.zip"}
    """
    print(f"[scrape] Fetching ZIP links from {page_url}")
    resp = requests.get(page_url, timeout=30)
    resp.raise_for_status()
    html = resp.text

    # Anchor href ending in raw_poultry_sampling_data_fyYYYY.zip
    pattern = re.compile(
        r'href=[\'"](?P<url>[^\'"]*raw_poultry_sampling_data_fy(?P<year>\d{4})\.zip)[\'"]',
        re.IGNORECASE
    )

    links: Dict[int, str] = {}
    for m in pattern.finditer(html):
        year = int(m.group("year"))
        url = m.group("url")
        if url.startswith("/"):
            url = f"https://www.fsis.usda.gov{url}"
        links[year] = url

    print(f"[scrape] Found ZIPs for years: {sorted(links.keys())}")
    return links


def _download_zip_to_memory(url: str) -> zipfile.ZipFile:
    """
    Download ZIP content into memory and return a ZipFile handle.
    """
    print(f"[download-memory] {url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9"
    }

    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()

    return zipfile.ZipFile(io.BytesIO(resp.content))


def _find_member_in_zip(z: zipfile.ZipFile, year: int) -> Optional[Tuple[str, bytes]]:
    """
    Search inside the ZIP for the expected FSIS JSON filename; if not found,
    return the first JSON/CSV member. Returns (filename, bytes) or None.
    """
    expected = f"usda_fsis_data_product_establishment_specific_laboratory_sampling_raw_poultry_product_fy{year}.json"

    # Prefer the long expected JSON name
    for name in z.namelist():
        if name.lower().endswith(expected.lower()):
            return name, z.read(name)

    # Fallback: any JSON or CSV
    for name in z.namelist():
        lower = name.lower()
        if lower.endswith(".json") or lower.endswith(".csv"):
            return name, z.read(name)

    return None


# ----------------------------
# JSON normalization helpers
# ----------------------------
def _expand_nested_lists(df: pd.DataFrame) -> pd.DataFrame:
    while True:
        list_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, list)).any()]
        if not list_cols:
            break
        for c in list_cols:
            df = df.explode(c, ignore_index=True)
    return df


def _json_to_df(records: Union[List[dict], Dict[str, Any]]) -> pd.DataFrame:
    """
    Flatten FSIS JSON records that may contain nested structures.
    """
    if isinstance(records, list) and records and isinstance(records[0], dict):
        df = pd.json_normalize(records, max_level=1)
    elif isinstance(records, dict):
        for key in ("records", "rows", "items", "data"):
            if key in records and isinstance(records[key], list):
                return _json_to_df(records[key])
        df = pd.json_normalize(records, max_level=1)
    else:
        df = pd.DataFrame({"value": records})

    df = _expand_nested_lists(df)

    while True:
        dict_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, dict)).any()]
        if not dict_cols:
            break
        for c in dict_cols:
            sub = pd.json_normalize(df[c]).add_prefix(f"{c}.")
            df = pd.concat(
                [df.drop(columns=[c]).reset_index(drop=True), sub.reset_index(drop=True)],
                axis=1
            )
    return df


def get_primary_table_data(root: Any) -> List[dict]:
    """
    Extract data.primary_table_data from FSIS JSON payloads.
    """
    if isinstance(root, dict):
        data = root.get("data")
        if isinstance(data, dict):
            ptd = data.get("primary_table_data")
            if isinstance(ptd, list):
                return ptd
            # Case variations / nested occurrences
            for k, v in data.items():
                if isinstance(k, str) and k.lower() == "primary_table_data" and isinstance(v, list):
                    return v
    if isinstance(root, list) and root:
        first = root[0]
        if isinstance(first, dict):
            data = first.get("data")
            if isinstance(data, dict) and isinstance(data.get("primary_table_data"), list):
                return data["primary_table_data"]
    raise ValueError("Could not find 'primary_table_data' in the JSON.")


# ----------------------------
# Universal in-memory loader
# ----------------------------
def _load_data_bytes(fname: str, data: bytes) -> pd.DataFrame:
    """
    Load either CSV (flat) or JSON (with primary_table_data) from in-memory bytes.
    """
    if fname.lower().endswith(".csv"):
        return pd.read_csv(io.BytesIO(data))
    elif fname.lower().endswith(".json"):
        root = json.loads(data.decode("utf-8"))
        primary_records = get_primary_table_data(root)
        return _json_to_df(primary_records)
    else:
        raise ValueError(f"Unsupported data type in ZIP: {fname}")


# ----------------------------
# Step 1 & 2: Download ZIPs to memory; extract & combine
# ----------------------------
def build_combined_df_in_memory(years: range) -> pd.DataFrame:
    zip_links = _get_fsis_zip_links(FSIS_PAGE_URL)
    dfs: List[pd.DataFrame] = []

    for y in years:
        url = zip_links.get(y)
        if not url:
            print(f"[warn] No ZIP URL for FY{y} on FSIS page; skipping.")
            continue

        z = _download_zip_to_memory(url)
        member = _find_member_in_zip(z, y)
        if not member:
            print(f"[warn] No JSON/CSV found inside FY{y} ZIP; skipping.")
            continue

        fname, data_bytes = member
        df_y = _load_data_bytes(fname, data_bytes)
        df_y["year"] = y
        dfs.append(df_y)
        print(f"[data] FY{y}: loaded {fname} ({len(df_y):,} rows)")

    if not dfs:
        raise RuntimeError("No yearly datasets were loaded; cannot build combined DataFrame.")

    combined = pd.concat(dfs, ignore_index=True)
    print(f"[combine] Combined rows: {len(combined):,}")
    return combined


# ----------------------------
# Step 3: Write combined raw to DuckDB
# ----------------------------
def write_raw_to_duckdb(df: pd.DataFrame, db_path: Path, raw_table_name: str) -> duckdb.DuckDBPyConnection:
    print(f"[duckdb] Writing RAW combined data to {db_path}::{raw_table_name}")
    _ensure_dir(db_path.parent)
    con = duckdb.connect(str(db_path))
    con.register("combined_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {raw_table_name} AS SELECT * FROM combined_df")
    return con


# ----------------------------
# Step 4: Clean, export, and store clean in DuckDB
# ----------------------------
def _unify_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle FSIS CSV/JSON header variants, then rename to short labels.
    """
    # Normalize ProjectName -> project_name (CSV variant)
    if "ProjectName" in df.columns and "project_name" not in df.columns:
        df = df.rename(columns={"ProjectName": "project_name"})

    variants = {
        'eid': ['establishment_id', 'EstablishmentID'],
        'enum': ['establishment_number', 'EstablishmentNumber'],
        'ename': ['establishment_name', 'EstablishmentName'],
        'estate': ['establishment_state', 'State'],
        'cam': ['campylobacter_analysis_30ml', 'CampylobacterAnalysis30mL', 'CampylobacterAnalysis'],
        'sal': ['salmonella_sp_analysis', 'SalmonellaSpAnalysis'],
        'pid': ['project_code', 'ProjectCode'],
    }

    rename_map: Dict[str, str] = {}
    for target, opts in variants.items():
        for opt in opts:
            if opt in df.columns:
                rename_map[opt] = target
                break

    return df.rename(columns=rename_map)


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace("NULL", None)
    df = _unify_column_names(df)

    # Reorder if available
    order = ['eid', 'pid', 'year', 'cam', 'sal']
    if set(order).issubset(df.columns):
        df = df[order + [c for c in df.columns if c not in order]]

    # Map Positive/Negative to 1/0; infer types
    df = df.replace({"Positive": 1, "Negative": 0})
    df = df.infer_objects()

    # Sort; cast cam/sal to nullable Int64
    sort_cols = [c for c in ["year", "eid"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(by=sort_cols)

    for c in ["cam", "sal"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')

    # Build pid from project_name frequency if present
    if "project_name" in df.columns:
        freq_order = df['project_name'].value_counts().index
        df['pid'] = (pd.Categorical(df['project_name'], categories=freq_order, ordered=True).codes + 1)

    return df


def export_files(df: pd.DataFrame) -> None:
    # CSV
    _ensure_dir(BASE_DIR)
    df.to_csv(OUT_CSV, index=False)
    print(f"[export] Cleaned CSV saved: {OUT_CSV}")

    # Excel (optional, mirrors your prior workflow)
    with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as xw:
        df.to_excel(xw, sheet_name="combined_data", index=False)
        ws = xw.sheets["combined_data"]
        if not df.empty:
            ws.autofilter(0, 0, len(df), df.shape[1] - 1)
    print(f"[export] Combined Excel saved: {OUT_XLSX}")


def write_clean_to_duckdb(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, clean_table_name: str) -> None:
    print(f"[duckdb] Writing CLEANED data to {DB_PATH}::{clean_table_name}")
    con.register("cleaned_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {clean_table_name} AS SELECT * FROM cleaned_df")


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    print("[step] 1+2: In-memory download of ZIPs, extract, and combine")
    _ensure_dir(BASE_DIR)
    combined_df = build_combined_df_in_memory(YEARS)

    print("[step] 3: Insert RAW combined into DuckDB (fsis.db)")
    con = write_raw_to_duckdb(combined_df, DB_PATH, RAW_TABLE)

    print("[step] 4: Clean and export")
    cleaned_df = clean_df(combined_df)
    export_files(cleaned_df)
    write_clean_to_duckdb(con, cleaned_df, CLEAN_TABLE)

    print("[done] Finished")

