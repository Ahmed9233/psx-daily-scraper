# Run this as a single cell in Jupyter

import requests
import pandas as pd
from datetime import datetime
import math
import os

# ---------- Helpers ----------
def safe_num(x):
    """Try to convert x to int/float, otherwise return None."""
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() in ("na", "n/a", "-", "--"):
            return None
        s = s.replace(",", "").replace("%", "").replace("—", "").replace("−", "-")
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1]
        f = float(s)
        if math.isclose(f, int(f)):
            return int(f)
        return f
    except Exception:
        return None

# ---------- Endpoints ----------
endpoints = {
    "DailyActivity_1": "https://www.scstrade.com/MarketStatistics/MS_DailyActivity.aspx/chartind",
    "DailyActivity_2": "https://www.scstrade.com/MarketStatistics/MS_DailyActivity.aspx/chartact",
}

headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ---------- Column mappings ----------
map_table1 = {
    "kse_index_type": "Name",
    "kse_index_open": "Open",
    "kse_index_high": "High",
    "kse_index_low": "Low",
    "kse_index_close": "Close",
    "kse_index_value": "Value",
    "kse_index_change": "Change"
}

# ---------- Fetch & Save ----------
excel_file = "PSX_DailyActivity.xlsx"

for table_name, url in endpoints.items():
    try:
        print(f"\n📡 Fetching {table_name} ...")
        resp = requests.post(url, headers=headers, timeout=30)
        data_json = resp.json()

        if isinstance(data_json, dict) and "d" in data_json:
            raw_list = data_json["d"]
        elif isinstance(data_json, list):
            raw_list = data_json
        else:
            raw_list = next((v for v in data_json.values() if isinstance(v, list)), None)

        if not raw_list:
            print(f"⚠️ No data found for {table_name}")
            continue

        df = pd.DataFrame(raw_list)

        # Normalize columns
        if table_name == "DailyActivity_1":
            df.rename(columns=map_table1, inplace=True)
        else:
            rename_map = {}
            for src in df.columns:
                low = src.lower()
                if "sector" in low: rename_map[src] = "SECTOR"
                elif "code" in low: rename_map[src] = "CODE"
                elif "name" in low: rename_map[src] = "NAME"
                elif "open" in low: rename_map[src] = "OPEN"
                elif "high" in low: rename_map[src] = "HIGH"
                elif "low" in low: rename_map[src] = "LOW"
                elif "close" in low: rename_map[src] = "CLOSE"
                elif "vol" in low: rename_map[src] = "VOLUME"
                elif "change" in low: rename_map[src] = "CHANGE"
            df.rename(columns=rename_map, inplace=True)

        # Add timestamp
        df["ScrapedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Append data to Excel (if exists)
        if os.path.exists(excel_file):
            with pd.ExcelWriter(excel_file, mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
                try:
                    old_df = pd.read_excel(excel_file, sheet_name=table_name)
                    combined = pd.concat([old_df, df], ignore_index=True)
                except Exception:
                    combined = df
                combined.to_excel(writer, index=False, sheet_name=table_name)
        else:
            with pd.ExcelWriter(excel_file, mode="w", engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=table_name)

        print(f"✅ Saved {len(df)} new rows to {table_name}")

    except Exception as e:
        print(f"❌ Error for {table_name}: {e}")

print(f"\n📁 File saved as: {os.path.abspath(excel_file)}")
