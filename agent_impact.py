import os
import re
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- Time configuration ---
now_local = datetime.now()
days_back = 1
today_date = now_local.strftime("%Y-%m-%d")
to_timestamp = int(now_local.timestamp() * 1000)
from_timestamp = int((now_local - timedelta(days=days_back)).timestamp() * 1000)

# --- Paths ---
output_dir = r"Output"
EXCEL_PATH = "Customer_Data.xlsx"
os.makedirs(output_dir, exist_ok=True)

# --- API URLs ---
AGENT_URL = "https://api-v2.7signal.com/kpis/agents"
AUTH_URL = "https://api-v2.7signal.com/oauth2/token"

# --- KPI Types ---
AGENT_KPI_TYPES = {
    "ROAMING": "roaming",
    "COVERAGE": "coverage",
    "CONGESTION": "congestion",
    "CO_CHANNEL_INTERFERENCE": "cci",
    "ADJACENT_CHANNEL_INTERFERENCE": "aci",
    "RF_PROBLEM": "rf_problem"
}

# --- Load customer details ---
customers_df = pd.read_excel(EXCEL_PATH)

def get_auth_token(client_id, client_secret):
    """Retrieve OAuth token using client credentials."""
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    try:
        response = requests.post(AUTH_URL, data=data, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.RequestException as e:
        print(f"❌ Auth error for {client_id}: {e}")
        return None

def fetch_kpi_data(kpi_type, kpi_prefix):
    """Fetch and process agent KPI data for all customers."""
    def fetch_customer_data(client_info):
        client_id, client_secret, account_name, vertical = client_info
        token = get_auth_token(client_id, client_secret)
        if not token:
            return None

        url = f"{AGENT_URL}?from={from_timestamp}&to={to_timestamp}&type={kpi_type}&includeClientCount=true"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            data = response.json().get("results", [])
            if not data:
                return None

            records = []
            for entry in data:
                client_count = entry.get("clientCount", 0)
                for type_info in entry.get("types", []):
                    records.append({
                        "account_name": account_name,
                        "vertical": vertical,
                        "client_count": client_count,
                        f"{kpi_prefix}_good_sum": type_info.get("goodSum", 0),
                        f"{kpi_prefix}_critical_sum": type_info.get("criticalSum", 0),
                        f"{kpi_prefix}_warning_sum": type_info.get("warningSum", 0),
                    })

            return pd.DataFrame(records)

        except requests.RequestException:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(tqdm(
            executor.map(fetch_customer_data,
                         zip(customers_df['client_id'],
                             customers_df['client_secret'],
                             customers_df['account_name'],
                             customers_df['vertical'])),
            total=len(customers_df), desc=f"Fetching {kpi_type}"
        ))

    data_frames = [df for df in results if df is not None]
    if not data_frames:
        return pd.DataFrame()

    df = pd.concat(data_frames, ignore_index=True)
    if "account_name" in df.columns and "vertical" in df.columns:
        grouped = df.groupby(["account_name", "vertical"], as_index=False).agg("sum")
        grouped[f"avg_{kpi_prefix}_minutes_per_client_per_day"] = (
            grouped[f"{kpi_prefix}_critical_sum"] / grouped["client_count"]
        ) / days_back
        return grouped
    else:
        print(f"⚠️ Missing expected columns in data for KPI type '{kpi_type}'")
        return pd.DataFrame()

# --- Fetch and merge all KPI types ---
final_df = customers_df[["account_name", "vertical"]].drop_duplicates()
client_counts = pd.DataFrame()

for i, (kpi_type, prefix) in enumerate(AGENT_KPI_TYPES.items()):
    kpi_df = fetch_kpi_data(kpi_type, prefix)
    if i == 0 and not kpi_df.empty:
        client_counts = kpi_df[["account_name", "vertical", "client_count"]]
    if not kpi_df.empty:
        final_df = final_df.merge(kpi_df.drop(columns=["client_count"]), on=["account_name", "vertical"], how="left")

# Merge client count once
if not client_counts.empty:
    final_df = final_df.merge(client_counts, on=["account_name", "vertical"], how="left")

# Add static metadata
final_df.insert(2, "days_back", days_back)

# Reorder client_count to third column
cols = final_df.columns.tolist()
if "client_count" in cols:
    cols.insert(3, cols.pop(cols.index("client_count")))
    final_df = final_df[cols]

# Rename KPI columns to include 'critical'
rename_map = {
    f"avg_{v}_minutes_per_client_per_day": f"avg_critical_{v}_minutes_per_client_per_day"
    for v in AGENT_KPI_TYPES.values()
}
final_df.rename(columns=rename_map, inplace=True)

# Calculate total average and waste
avg_columns = list(rename_map.values())
final_df["total_avg_critical_minutes_per_client_per_day"] = final_df[avg_columns].sum(axis=1)
final_df["total_avg_critical_hours_per_client_per_day"] = (
    final_df["total_avg_critical_minutes_per_client_per_day"] / 60
)
final_df["waste_identified_in_hours_per_day_by_agents"] = (
    final_df["total_avg_critical_hours_per_client_per_day"] * final_df["client_count"]
)

# Drop good_sum and warning_sum columns
final_df = final_df.drop(columns=[col for col in final_df.columns if "good_sum" in col or "warning_sum" in col])

# Define user-friendly column names
user_friendly_names = {
    "account_name": "Account Name",
    "vertical": "Industry Sector",
    "days_back": "Days of Data",
    "client_count": "Number of Clients",
    "avg_critical_roaming_minutes_per_client_per_day": "Average Critical Roaming Minutes per Client per Day",
    "avg_critical_coverage_minutes_per_client_per_day": "Average Critical Coverage Minutes per Client per Day",
    "avg_critical_congestion_minutes_per_client_per_day": "Average Critical Congestion Minutes per Client per Day",
    "avg_critical_cci_minutes_per_client_per_day": "Average Critical Co-Channel Interference Minutes per Client per Day",
    "avg_critical_aci_minutes_per_client_per_day": "Average Critical Adjacent Channel Interference Minutes per Client per Day",
    "avg_critical_rf_problem_minutes_per_client_per_day": "Average Critical RF Problem Minutes per Client per Day",
    "total_avg_critical_minutes_per_client_per_day": "Total Average Critical Minutes per Client per Day",
    "total_avg_critical_hours_per_client_per_day": "Total Average Critical Hours per Client per Day",
    "waste_identified_in_hours_per_day_by_agents": "Total Critical Hours Identified per Day",
    "roaming_critical_sum": "Total Critical Roaming Minutes",
    "coverage_critical_sum": "Total Critical Coverage Minutes",
    "congestion_critical_sum": "Total Critical Congestion Minutes",
    "cci_critical_sum": "Total Critical Co-Channel Interference Minutes",
    "aci_critical_sum": "Total Critical Adjacent Channel Interference Minutes",
    "rf_problem_critical_sum": "Total Critical RF Problem Minutes"
}

# Apply user-friendly names to final_df
final_df.rename(columns=user_friendly_names, inplace=True)

# Save account-level output
output_path = os.path.join(output_dir, f"agent_impact_per_account_{days_back}_days_back_of_data_from_{today_date}.csv")
final_df.to_csv(output_path, index=False)

# --- Generate per-vertical output ---
vertical_df = final_df.copy()

# Split out client count separately for sum
if "Number of Clients" in vertical_df.columns:
    client_sum_df = vertical_df.groupby("Industry Sector", as_index=False)["Number of Clients"].sum()

# Average all other numeric columns
numeric_cols = vertical_df.select_dtypes(include='number').columns.tolist()
numeric_cols = [col for col in numeric_cols if col != "Number of Clients"]

grouped_vertical_df = vertical_df.groupby("Industry Sector", as_index=False)[numeric_cols].mean()

# Merge summed client counts back in
if not client_sum_df.empty:
    grouped_vertical_df = grouped_vertical_df.merge(client_sum_df, on="Industry Sector", how="left")

# Drop if already exists (to avoid ValueError)
if "Days of Data" in grouped_vertical_df.columns:
    grouped_vertical_df.drop(columns=["Days of Data"], inplace=True)

# Re-insert static metadata
grouped_vertical_df.insert(1, "Days of Data", days_back)

# Save vertical-level CSV
vertical_output_filename = f"agent_impact_per_vertical_{days_back}_days_back_of_data_from_{today_date}.csv"
vertical_output_path = os.path.join(output_dir, vertical_output_filename)
grouped_vertical_df.to_csv(vertical_output_path, index=False)

print(f"✅ Account-level agent report saved: {output_path}")
print(f"✅ Vertical-level agent report saved: {vertical_output_path}")
