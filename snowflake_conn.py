import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import re
from datetime import datetime
import sys

# ------------------------------
# Read sources from Excel
# ------------------------------
def read_sources(excel_file):
    df = pd.read_excel(excel_file)
    # expecting columns: source_type, name, url
    return df


# ------------------------------
# Extract data
# ------------------------------
def extract_data(sources):
    data = {}
    for _, row in sources.iterrows():
        name = row["name"]
        url = row["url"]
        source_type = row["source_type"].lower()
        print(name)

        if source_type == "api":
            print("Before API call")
            response = requests.get(url)
            print("After API call")           
            
            response.raise_for_status()
            df = pd.DataFrame(response.json())
            print(df)
        elif source_type == "file":
            if url.endswith(".csv"):
                df = pd.read_csv(url)
            else:
                df = pd.read_parquet(url, engine="pyarrow")
        else:
            raise ValueError(f"Unknown source_type: {source_type}")

        data[name] = df
        print(f"Extracted {len(df)} rows from {name}")
    return data


# ------------------------------
# Clean column names for Snowflake
# ------------------------------
def clean_column_names(df):
    df.columns = [
        re.sub(r'[^0-9A-Z_]', '', col.upper().replace(" ", "_"))
        for col in df.columns
    ]
    return df


# ------------------------------
# Load to Snowflake (all VARCHAR)
# ------------------------------
def load_to_snowflake(dataframes):
    conn = snowflake.connector.connect(
        user="Team4",
        password="ProductHackTeam4",
        account="xpjgnka-chb32659",
        warehouse="COMPUTE_WH",
        database="HACK",
        schema="STAGE", 
        role="ACCOUNTADMIN"
    )
    
    cs = conn.cursor()
    try:
        for table_name, df in dataframes.items():
            # 1. Clean column names
            df = clean_column_names(df)

            # 2. Cast everything to string
            df = df.astype(str).fillna("")
            df["LOAD_DATE"] = pd.to_datetime(datetime.now().replace(tzinfo=None))

            # Ensure correct dtype for Snowflake (TIMESTAMP_NTZ)
            df["LOAD_DATE"] = df["LOAD_DATE"].astype("datetime64[ns]")

            table_name_sf = table_name.upper()
            
            # 3. Create table with all VARCHAR cols
            create_stmt = f"""
                CREATE TABLE IF NOT EXISTS {table_name_sf} (
                    {', '.join([f'{col} VARCHAR' for col in df.columns])}
                )
            """
            cs.execute(create_stmt)

            # 4. Load into Snowflake
            success, nchunks, nrows, _ = write_pandas(conn, df, table_name_sf, overwrite=False)
            print(f"Loaded {nrows} rows into {table_name_sf} (success={success})")
    finally:
        cs.close()
        conn.close()


# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("No file path provided")
        sys.exit(1)

    excel_file = sys.argv[1]
    print(f"Processing file: {excel_file}")
    sources = read_sources(excel_file)
    data = extract_data(sources)
    load_to_snowflake(data)
