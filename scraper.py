# Run this as a single cell in Jupyter (restart kernel before running if needed)

import requests
import pandas as pd
import pyodbc
from datetime import datetime
import time
import math

# ---------- Helpers ----------
def safe_num(x):
    """Try to convert x to int (if whole) or float, otherwise return None."""
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() in ("na", "n/a", "-", "--"):
            return None
        # remove commas and percent signs and other thousand separators
        s = s.replace(",", "").replace("%", "").replace("—", "").replace("−", "-")
        # handle parentheses negative like (12)
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1]
        # if it contains any non-numeric except . and - then try to strip currency
        # attempt float conversion
        f = float(s)
        if math.isclose(f, int(f)):
            return int(f)
        return f
    except Exception:
        return None

def ensure_scrapedat(cursor, table_name):
    """Ensure ScrapedAt DATETIME column exists; add it if missing."""
    check_sql = f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name.replace('[','').replace(']','').split('.')[-1]}'
        AND COLUMN_NAME = 'ScrapedAt';
    """
    cursor.execute(check_sql)
    exists = cursor.fetchone()[0]
    if exists == 0:
        alter = f"ALTER TABLE {table_name} ADD ScrapedAt DATETIME;"
        print(f"🔧 Adding ScrapedAt column to {table_name}")
        cursor.execute(alter)
        cursor.connection.commit()

# ---------- DB Connection ----------
conn = pyodbc.connect(
    r"Driver={SQL Server};"
    r"Server=DESKTOP-Q8V3M1P\SQLEXPRESS;"
    r"Database=PSX_SHARES_DAILY;"
    r"Trusted_Connection=yes;"
)
cursor = conn.cursor()

# ---------- Endpoints ----------
endpoints = {
    "DailyActivity_1": "https://www.scstrade.com/MarketStatistics/MS_DailyActivity.aspx/chartind",
    "DailyActivity_2": "https://www.scstrade.com/MarketStatistics/MS_DailyActivity.aspx/chartact",
}

# Common POST headers that mimic browser XHR
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36"
}

# ---------- Column mappings ----------
# Map API keys to SQL column names for table 1 (indices)
map_table1 = {
    "kse_index_type": "Name",
    "kse_index_open": "Open",
    "kse_index_high": "High",
    "kse_index_low": "Low",
    "kse_index_close": "Close",
    "kse_index_value": "Value",
    "kse_index_change": "Change"
}
# For table2 (activities), common keys — if different, code will adapt by printing df.columns
map_table2 = {
    "sector": "SECTOR",      # example fallback; will inspect actual keys below
    "code": "CODE",
    "name": "NAME",
    "open": "OPEN",
    "high": "HIGH",
    "low": "LOW",
    "close": "CLOSE",
    "volume": "VOLUME",
    "change": "CHANGE"
}

# ---------- Fetch & Insert ----------
for table_name, url in endpoints.items():
    try:
        print(f"\n📡 Fetching {url} ...")
        # POST request (no payload required for this endpoint)
        resp = requests.post(url, headers=headers, timeout=30)
        print("HTTP", resp.status_code)
        text = resp.text.strip()

        # Try to parse JSON safely
        data_json = None
        try:
            data_json = resp.json()
        except Exception:
            # sometimes server returns HTML or wrapped content; try to find JSON substring
            # but for these endpoints resp.json() should usually work when called as POST
            print("⚠️ Response not direct JSON. Previewing text (first 400 chars):")
            print(text[:400])
            # can't parse -> skip this endpoint
            continue

        # The endpoint may return {'d': [...] } or direct list
        raw_list = None
        if isinstance(data_json, dict) and "d" in data_json:
            raw_list = data_json["d"]
        elif isinstance(data_json, list):
            raw_list = data_json
        else:
            # Sometimes nested; try to find first list inside dict
            found = None
            for v in data_json.values():
                if isinstance(v, list):
                    found = v
                    break
            raw_list = found

        if not raw_list:
            print("⚠️ No usable list found in response JSON for", table_name)
            continue

        df = pd.DataFrame(raw_list)
        print("✅ fetched rows:", len(df))
        print("columns from API:", list(df.columns)[:40])

        # Decide mapping and target SQL columns
        if table_name == "DailyActivity_1":
            # Rename known keys if present
            df.rename(columns=map_table1, inplace=True)
            expected_cols = ["Name", "Open", "High", "Low", "Close", "Value", "Change"]
            missing = [c for c in expected_cols if c not in df.columns]
            if missing:
                print("⚠️ Warning - expected columns missing after rename:", missing)
                print("API columns available:", list(df.columns))
                # try to infer columns by containing substrings
                for src in list(df.columns):
                    low = src.lower()
                    if "type" in low and "Name" not in df.columns:
                        df.rename(columns={src: "Name"}, inplace=True)
                    if "open" in low and "Open" not in df.columns:
                        df.rename(columns={src: "Open"}, inplace=True)
                    if "high" in low and "High" not in df.columns:
                        df.rename(columns={src: "High"}, inplace=True)
                    if "low" in low and "Low" not in df.columns:
                        df.rename(columns={src: "Low"}, inplace=True)
                    if "close" in low and "Close" not in df.columns:
                        df.rename(columns={src: "Close"}, inplace=True)
                    if "value" in low and "Value" not in df.columns:
                        df.rename(columns={src: "Value"}, inplace=True)
                    if "change" in low and "Change" not in df.columns:
                        df.rename(columns={src: "Change"}, inplace=True)
        else:
            # For DailyActivity_2 — try to normalize keys to SQL expected names (upper)
            # We'll map by detecting keywords
            rename_map = {}
            for src in df.columns:
                low = src.lower()
                if "sector" in low and "SECTOR" not in df.columns:
                    rename_map[src] = "SECTOR"
                elif "code" in low and "CODE" not in df.columns:
                    rename_map[src] = "CODE"
                elif "name" in low and "NAME" not in df.columns:
                    rename_map[src] = "NAME"
                elif "open" in low and "OPEN" not in df.columns:
                    rename_map[src] = "OPEN"
                elif "high" in low and "HIGH" not in df.columns:
                    rename_map[src] = "HIGH"
                elif "low" in low and "LOW" not in df.columns:
                    rename_map[src] = "LOW"
                elif "close" in low and "CLOSE" not in df.columns:
                    rename_map[src] = "CLOSE"
                elif "vol" in low and "VOLUME" not in df.columns:
                    rename_map[src] = "VOLUME"
                elif "change" in low and "CHANGE" not in df.columns:
                    rename_map[src] = "CHANGE"
            if rename_map:
                df.rename(columns=rename_map, inplace=True)

        # show first rows for checking
        print("Preview (first 3 rows):")
        display_df = df.head(3)
        print(display_df.to_string(index=False))

        if df.empty:
            print("No rows to insert for", table_name)
            continue

        # Ensure ScrapedAt exists in SQL table
        ensure_scrapedat(cursor, table_name if table_name.startswith("[") else f"[dbo].[{table_name}]")

        # Prepare insert: match SQL order (use the columns present in df that match expected)
        if table_name == "DailyActivity_1":
            sql_cols = ["Name", "Open", "High", "Low", "Close", "Value", "Change", "ScrapedAt"]
        else:
            # for table2 try to use common order if available
            # adjust to uppercase names used when creating table earlier (DailyActivity_2 used uppercase)
            # Use the intersection between available df columns and desired order:
            desired = ["SECTOR", "CODE", "NAME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "CHANGE"]
            # but df might hold these names in different case; normalize:
            df_columns_upper = {c: c for c in df.columns}
            # create a column lookup map from expected -> actual column name in df
            lookup = {}
            for exp in desired:
                for actual in df.columns:
                    if actual.strip().upper() == exp:
                        lookup[exp] = actual
                        break
            # build sql_cols from matched ones
            sql_cols = [lookup[exp] for exp in desired if exp in lookup] + ["ScrapedAt"]

        # Insert rows
        inserted = 0
        for _, row in df.iterrows():
            values = []
            for col in sql_cols:
                if col == "ScrapedAt":
                    values.append(datetime.now())
                    continue
                # actual df column name may differ in case; find it
                actual_col = col
                if col not in df.columns:
                    # try case-insensitive match
                    found = None
                    for c in df.columns:
                        if c.strip().upper() == col.strip().upper():
                            found = c
                            break
                    actual_col = found if found else col
                v = row.get(actual_col, None) if actual_col in df.columns else None

                # for Name — keep string
                if col.lower() in ("name", "sector", "code", "sector"):
                    values.append(None if pd.isna(v) else str(v).strip())
                else:
                    # numeric
                    nv = safe_num(v)
                    # If the target column in DB is int but we got None, insert None acceptable
                    values.append(nv)
            # Build dynamic insert query with parameter placeholders
            placeholders = ",".join("?" for _ in sql_cols)
            cols_sql = ",".join(f"[{c}]" for c in sql_cols)
            insert_sql = f"INSERT INTO [dbo].[{table_name}] ({cols_sql}) VALUES ({placeholders})"
            try:
                cursor.execute(insert_sql, tuple(values))
                inserted += 1
            except Exception as e:
                print("❌ Insert error for row:", e)
                print("Query:", insert_sql)
                print("Values sample:", values[:5])
                # continue with next row
        conn.commit()
        print(f"💾 Inserted {inserted} rows into {table_name}")

    except requests.exceptions.RequestException as re:
        print("Network error when fetching", url, re)
    except Exception as e:
        print("General error for", table_name, e)

# Cleanup
cursor.close()
conn.close()
print("\n✅ All done.")