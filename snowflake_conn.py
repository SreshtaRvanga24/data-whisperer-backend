# pip install pandas requests snowflake-connector-python pyarrow
# Optional (recommended): pip install phonenumbers

import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import re
from datetime import datetime, timezone, date as date_cls
import sys
import logging
import warnings
import numpy as np

# ============= Optional robust phone lib with fallback ============
try:
    import phonenumbers as pn  # robust library
except ModuleNotFoundError:
    pn = None
    print("WARN: 'phonenumbers' not installed; using basic phone normalization fallback.")
# =================================================================

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.ERROR)

# ==============================
# NULL / CLEAN HELPERS
# ==============================
NULL_STRINGS = {
    "", " ", "nan", "none", "null", "na", "n/a", "<na>", "nat", "na t", "n.a.",
    "nil", "missing", "undefined"
}

def nullify(value):
    """Normalize common NA-like strings and blanks to None (real NULL in Snowflake)."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    s = str(value).strip()
    return None if s.lower() in NULL_STRINGS else (s if s != "" else None)

def nullify_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply nullify() to object/string columns only; DO NOT blanket-replace across the frame,
    so datetime64 columns keep NaT (avoids int epoch issues later).
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            out[col] = out[col].map(nullify)
    return out

def strip_strings_inplace(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace for string-like columns without forcing type conversion."""
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def safeguard_bigints(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prevent Arrow overflow: keep >18-digit ints as strings so write_pandas doesn't cast to int64.
    """
    out = df.copy()
    big_int_pattern = re.compile(r'^-?\d{19,}$')
    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            def _keep_str_if_bigint(x):
                if isinstance(x, int):
                    return str(x) if len(str(abs(x))) > 18 else x
                if isinstance(x, str) and big_int_pattern.match(x):
                    return x  # keep as string
                return x
            out[col] = out[col].map(_keep_str_if_bigint)
    return out

# ==============================
# PHONE NORMALIZATION HELPERS
# ==============================
REGION_ALIASES = {
    "us": "US", "usa": "US", "united states": "US", "united states of america": "US",
    "uk": "GB", "gb": "GB", "great britain": "GB", "united kingdom": "GB",
    "ca": "CA", "canada": "CA"
}
DEFAULT_REGIONS = ["US", "GB", "CA"]  # try in this order if no hint and no leading '+'

def _preclean_phone(raw: str) -> str:
    """
    Keep only digits and a single leading '+'. Remove spaces, dashes, dots, parentheses,
    common extension markers (x123 / ext.123), convert 00/011 to '+',
    and drop the UK-style optional trunk '(0)'.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    s = re.sub(r'(?:\s*(?:ext\.?|x)\s*\d+)\s*$', '', s, flags=re.IGNORECASE)  # drop ext
    s = s.replace('(0)', '')                                                  # drop UK (0)
    s = re.sub(r'^\s*(?:00|011)\s*', '+', s)                                  # 00/011 -> +
    s = re.sub(r'[^\d+]', '', s)                                              # keep + and digits
    if s.count('+') > 1:
        s = '+' + s.replace('+', '')
    return s

def _infer_region_from_row(row_dict) -> str | None:
    for key in ("COUNTRY_CODE", "COUNTRY"):
        if key in row_dict and row_dict[key]:
            val = str(row_dict[key]).strip().lower()
            if val in REGION_ALIASES:
                return REGION_ALIASES[val]
            if len(val) == 2:  # ISO-2 provided
                return val.upper()
    return None

def _fallback_normalize(cleaned: str, row=None):
    """Lightweight E.164 normalizer when 'phonenumbers' is unavailable."""
    if cleaned == "":
        return None, False, "empty"
    if cleaned.startswith('+'):
        digits = cleaned[1:]
        if 8 <= len(digits) <= 15 and not digits.startswith('0'):
            return '+' + digits, True, "valid_basic"
        return None, False, "invalid_length"

    region = _infer_region_from_row(row or {}) or DEFAULT_REGIONS[0]
    digits = re.sub(r'\D', '', cleaned)

    if region in ("US", "CA"):
        if len(digits) == 10:
            return '+1' + digits, True, "valid_basic"
        if len(digits) == 11 and digits.startswith('1'):
            return '+' + digits, True, "valid_basic"
        return None, False, "invalid_usca"

    if region == "GB":
        if digits.startswith('0'):
            digits = digits[1:]
        if 9 <= len(digits) <= 10:
            return '+44' + digits, True, "valid_basic"
        return None, False, "invalid_gb"

    if 8 <= len(digits) <= 15 and not digits.startswith('0'):
        return '+' + digits, True, "valid_basic"
    return None, False, "invalid_generic"

def normalize_phone_any(raw, row=None):
    cleaned = _preclean_phone(raw)
    if pn is None:
        return _fallback_normalize(cleaned, row)

    if cleaned == "":
        return None, False, "empty"

    try_orders = []
    if cleaned.startswith('+'):
        try_orders = [None]
    else:
        hinted = _infer_region_from_row(row if row is not None else {})
        if hinted:
            try_orders = [hinted]
        try_orders.extend([r for r in DEFAULT_REGIONS if r != hinted])

    for region in try_orders:
        try:
            num = pn.parse(cleaned, region) if region else pn.parse(cleaned)
            if pn.is_valid_number(num):
                return pn.format_number(num, pn.PhoneNumberFormat.E164), True, "valid"
        except Exception:
            pass

    return None, False, "invalid"

def add_phone_columns(df: pd.DataFrame, phone_col="PHONE") -> pd.DataFrame:
    if phone_col not in df.columns:
        return df
    out = df.copy()
    if "PHONE_RAW" not in out.columns:
        out.insert(out.columns.get_loc(phone_col), "PHONE_RAW", out[phone_col])
    rows_as_dict = out.to_dict(orient="records")
    normalized = [normalize_phone_any(val, row=rows_as_dict[i]) for i, val in enumerate(out[phone_col].tolist())]
    out["PHONE_E164"]   = [t[0] for t in normalized]
    out["PHONE_VALID"]  = [t[1] for t in normalized]
    out["PHONE_REASON"] = [t[2] for t in normalized]
    return out

# ==============================
# TYPE COERCION HELPERS
# ==============================
def to_nullable_int(series: pd.Series) -> pd.Series:
    """
    Convert to pandas nullable integer (Int64). '123'/'123.0'/123.0 -> 123.
    Non-integers (e.g., '123.45') become <NA> to avoid silent truncation.
    """
    s = pd.to_numeric(series, errors="coerce")
    whole_mask = s.isna() | (s == np.floor(s))
    s = s.where(whole_mask, other=np.nan)
    return s.astype("Int64")

# ==============================
# TEMPORAL COERCION
# ==============================
from pandas.api.types import is_datetime64_any_dtype, is_object_dtype

DATE_ONLY_RX = re.compile(r"(?:^|_)(DATE|DOB|BIRTH|START_DATE|END_DATE|EFFECTIVE_DATE)(?:$|_)", re.I)
DATETIME_RX  = re.compile(r"(?:_AT$|_TS$|TIMESTAMP|TIME$)", re.I)

def coerce_temporal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert obvious date/datetime columns from strings so write_pandas
    creates TIMESTAMP_NTZ/DATE columns in Snowflake.
    """
    out = df.copy()
    for col in out.columns:
        u = col.upper()
        if u == "LOAD_DATE":
            out[col] = pd.to_datetime(out[col], errors="coerce")  # datetime64[ns]
            continue
        if DATETIME_RX.search(u):
            out[col] = pd.to_datetime(out[col], errors="coerce")  # datetime64[ns]
            continue
        if DATE_ONLY_RX.search(u):
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.date  # Python date -> Snowflake DATE
            continue
    return out

def nullify_non_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace missing values with None ONLY for object/string columns.
    Keep datetime columns as NaT so Arrow/Snowflake map them to TIMESTAMP/DATE.
    """
    out = df.copy()
    for col in out.columns:
        if is_datetime64_any_dtype(out[col]):
            continue  # keep NaT
        if is_object_dtype(out[col]):
            out[col] = out[col].where(pd.notnull(out[col]), None)
    return out

# ==============================
# SCHEMA CONTROL FOR CLEANSED (NEW)
# ==============================
def is_date_object_series(s: pd.Series) -> bool:
    if s.dtype != 'object':
        return False
    for v in s.dropna().head(20):
        if isinstance(v, date_cls):
            return True
        # if you accidentally have datetime objects in object dtype
        if isinstance(v, datetime):
            return True
    return False

def sf_type_for_column(name: str, s: pd.Series) -> str:
    u = name.upper()
    if u == "LOAD_DATE":
        return "TIMESTAMP_NTZ(9)"
    # pandas datetime64 -> TIMESTAMP_NTZ
    if is_datetime64_any_dtype(s):
        return "TIMESTAMP_NTZ(9)"
    # Python date/datetime objects stored in object dtype -> DATE or TIMESTAMP
    if is_date_object_series(s):
        # if any pure date objects, DATE; if any datetime, timestamp (handled above)
        # here it's OK to return DATE; TIMESTAMP comes via datetime64 branch
        # choose DATE for date-only columns
        # (we coerced *_DATE to .dt.date, so object-of-date -> DATE)
        return "DATE"
    if pd.api.types.is_bool_dtype(s):
        return "BOOLEAN"
    # pandas nullable int
    if str(s.dtype) == "Int64" or pd.api.types.is_integer_dtype(s):
        return "NUMBER(38,0)"
    if pd.api.types.is_float_dtype(s):
        return "FLOAT"
    # default
    return "VARCHAR"

def create_or_replace_cleansed_table(conn, database: str, schema: str, table: str, df: pd.DataFrame):
    cols = []
    for col in df.columns:
        cols.append(f'"{col}" {sf_type_for_column(col, df[col])}')
    ddl = f'CREATE OR REPLACE TABLE "{database}"."{schema}"."{table}" ({", ".join(cols)})'
    cur = conn.cursor()
    try:
        cur.execute(ddl)
    finally:
        cur.close()

# ==============================
# CORE ETL FUNCTIONS
# ==============================
def read_sources(excel_file):
    df = pd.read_excel(excel_file)
    # expecting columns: source_type, name, url
    return df

def extract_data(sources):
    data = {}
    for _, row in sources.iterrows():
        name = row["name"]
        url = row["url"]
        source_type = row["source_type"].lower()
        print(f"Processing: {name}")

        try:
            if source_type == "api":
                print("Making API call...")
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                df = pd.DataFrame(response.json())
                print(f"API call successful: {len(df)} rows")
            elif source_type == "file":
                if url.endswith(".csv"):
                    df = pd.read_csv(url)
                else:
                    df = pd.read_parquet(url, engine="pyarrow")
            else:
                raise ValueError(f"Unknown source_type: {source_type}")

            data[name] = df
            print(f"Extracted {len(df)} rows from {name}")

        except Exception as e:
            print(f"ERROR extracting from {name}: {e}")
            continue

    return data

def clean_column_names(df):
    df = df.copy()
    df.columns = [
        re.sub(r'[^0-9A-Z_]', '', col.upper().replace(" ", "_"))
        for col in df.columns
    ]
    return df

def process_dataframe_for_load(df):
    """Minimal processing before STAGE load."""
    out = df.copy()
    out = strip_strings_inplace(out)
    out = nullify_frame(out)  # now only touches object/string columns
    return out

def load_to_snowflake(dataframes):
    conn = None
    cursor = None

    try:
        conn = snowflake.connector.connect(
            user="Team4",
            password="ProductHackTeam4",
            account="xpjgnka-chb32659",
            warehouse="COMPUTE_WH",
            database="HACK",
            schema="STAGE",
            role="ACCOUNTADMIN",
            client_session_keep_alive=True,
            network_timeout=120,
            login_timeout=60,
            socket_timeout=120
        )
        cursor = conn.cursor()

        # One batch timestamp per run, as ISO string for STAGE (safe & indexable)
        batch_ts_iso = datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # ms precision

        # -----------------------------
        # Part 1: Load to STAGE (LOAD_DATE as VARCHAR ISO string)
        # -----------------------------
        print(f"Loading to STAGE with LOAD_DATE (VARCHAR) = {batch_ts_iso} ...")

        for table_name, df in dataframes.items():
            try:
                print(f"\nLoading {table_name} to STAGE...")

                df = df.reset_index(drop=True)
                df = clean_column_names(df)
                df = process_dataframe_for_load(df)

                # If input already had LOAD_DATE, preserve it as SRC_LOAD_DATE
                if "LOAD_DATE" in df.columns:
                    df.rename(columns={"LOAD_DATE": "SRC_LOAD_DATE"}, inplace=True)

                # Add our batch LOAD_DATE (string)
                df["LOAD_DATE"] = batch_ts_iso

                table_name_sf = table_name.upper()

                # Drop and recreate table (ALL VARCHAR, including LOAD_DATE)
                cursor.execute(f'DROP TABLE IF EXISTS "{table_name_sf}"')
                varchar_cols = [f'"{col}" VARCHAR' for col in df.columns]
                create_stmt = f'CREATE TABLE "{table_name_sf}" ({", ".join(varchar_cols)})'
                cursor.execute(create_stmt)

                # Load data to STAGE
                print(f"Loading DataFrame with shape {df.shape} into STAGE.{table_name_sf}...")
                write_pandas(
                    conn,
                    df,
                    table_name_sf,
                    auto_create_table=False,
                    overwrite=False,
                    quote_identifiers=True
                )
                print(f"SUCCESS: Loaded {len(df)} rows into STAGE.{table_name_sf}")

            except Exception as e:
                print(f"ERROR loading {table_name} to STAGE: {e}")
                continue

        # -----------------------------
        # Part 2: Transform STAGE -> CLEANSED
        # Only latest batch per table using TO_TIMESTAMP_NTZ(LOAD_DATE)
        # -----------------------------

        cursor.execute("SHOW TABLES IN HACK.STAGE")
        tables = [row[1] for row in cursor.fetchall()]
        if not tables:
            print("No tables found in HACK.STAGE to process.")
            return

        print(f"Found tables: {tables}")

        for t in tables:
            try:
                print(f"\nProcessing table: {t} ...")

                cursor.execute(f'DESCRIBE TABLE "HACK"."STAGE"."{t}"')
                table_desc = cursor.fetchall()
                all_columns = [row[0] for row in table_desc]
                print(f"All columns in {t}: {all_columns}")

                # Build SELECT column list EXCLUDING LOAD_DATE
                select_cols_list = [f'"{c}"' for c in all_columns if c != "LOAD_DATE"]
                select_cols = ", ".join(select_cols_list) if select_cols_list else "*"

                # Use LOAD_DATE only for filtering latest batch if it exists
                if "LOAD_DATE" in all_columns:
                    select_stmt = (
                        f'SELECT {select_cols} '
                        f'FROM "HACK"."STAGE"."{t}" '
                        f'WHERE TO_TIMESTAMP_NTZ("LOAD_DATE") = ('
                        f'  SELECT MAX(TO_TIMESTAMP_NTZ("LOAD_DATE")) FROM "HACK"."STAGE"."{t}"'
                        f')'
                    )
                else:
                    select_stmt = f'SELECT {select_cols} FROM "HACK"."STAGE"."{t}"'

                print(f"Fetching latest-batch records (without LOAD_DATE) from STAGE.{t}...")
                cursor.execute(select_stmt)
                rows = cursor.fetchall()

                df = pd.DataFrame(rows, columns=[row[0] for row in cursor.description]).reset_index(drop=True)
                print(f"Fetched {len(df)} rows")

                if len(df) == 0:
                    print(f"No records in latest batch for {t}, skipping...")
                    continue

                # Clean & transform (LOAD_DATE is not present in df anymore)
                print("Cleaning data...")
                df = strip_strings_inplace(df)
                df = nullify_frame(df)            # touches only object/string
                df = coerce_temporal_columns(df)  # safe even if LOAD_DATE is absent
                t_upper = t.upper()

                # DIM_CUSTOMERS: phone normalization
                if t_upper == "DIM_CUSTOMERS":
                    df = add_phone_columns(df, phone_col="PHONE")

                # LOYALTY_LEDGER: TXN_ID -> nullable integer
                if t_upper == "LOYALTY_LEDGER" and "TXN_ID" in df.columns:
                    df["TXN_ID"] = to_nullable_int(df["TXN_ID"])

                # Big-int safeguard for string columns
                df = safeguard_bigints(df)

                # Deduplicate
                before = len(df)
                df.drop_duplicates(inplace=True)
                df.reset_index(drop=True, inplace=True)
                after = len(df)
                if before != after:
                    print(f"Deduplicated - removed {before - after} duplicate rows")

                # Preserve datetime dtypes; only set None on object columns
                df = nullify_non_datetime(df)

                # (Re)create CLEANSED table WITHOUT LOAD_DATE (since df has no LOAD_DATE)
                print("Creating CLEANSED table with explicit types (no LOAD_DATE)...")
                create_or_replace_cleansed_table(
                    conn, database="HACK", schema="CLEANSED",
                    table=t_upper, df=df
                )

                # Load to CLEANSED (no auto-create; schema already correct)
                print(f"Writing {len(df)} rows to CLEANSED.{t_upper}...")
                write_pandas(
                    conn,
                    df,
                    table_name=t_upper,
                    database="HACK",
                    schema="CLEANSED",
                    auto_create_table=False,    # we're controlling schema
                    overwrite=False,            # table just got replaced above
                    quote_identifiers=True
                )
                print(f"SUCCESS: Wrote {len(df)} rows into HACK.CLEANSED.{t_upper}")

            except Exception as e:
                print(f"ERROR processing table {t}: {e}")
                continue

    except snowflake.connector.errors.Error as e:
        print(f"Snowflake connection error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("\n" + "="*50)
        print("CONNECTION CLOSED")
        print("="*50)


# ==============================
# Main
# ==============================
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <excel_file_path>")
        sys.exit(1)

    excel_file = sys.argv[1]
    print("="*50)
    print(f"STARTING ETL PROCESS")
    print(f"File: {excel_file}")
    print("="*50)

    try:
        sources = read_sources(excel_file)
        print(f"Found {len(sources)} data sources")

        data = extract_data(sources)
        if data:
            print(f"\nSuccessfully extracted data from {len(data)} sources")
            load_to_snowflake(data)
        else:
            print("No data extracted, skipping Snowflake load.")

    except Exception as e:
        print(f"ERROR: SCRIPT FAILED: {e}")
        sys.exit(1)

    print("\n" + "="*50)
    print("ETL PROCESS COMPLETED")
    print("="*25)