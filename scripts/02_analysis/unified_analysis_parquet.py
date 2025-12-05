# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
import glob 
import sys 

# --- System Path Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
PROJECT_ROOT_DIR = os.path.join(SCRIPT_DIR, '..', '..')

# --- Input Data Path Configuration ---
AIR_QUALITY_BASE_PATH = os.path.join(PROJECT_ROOT_DIR, 'data', 'preprocessed_data') 
POWER_DATA_CSV = os.path.join(PROJECT_ROOT_DIR, 'data', 'raw', 'kepco_thermal_power_monthly.csv')

# --- Analysis Target Configuration ---
TARGET_POLLUTANT = 'national_avg_PM10' # Dependent Variable: PM10
PM10_RAW_COL = 'pm10_z'                # Column name in raw Parquet data

# --- Result File Configuration ---
OUTPUT_DIR = os.path.join(PROJECT_ROOT_DIR, 'results', 'pandas_analysis')
OUTPUT_LOCAL_MERGED_CSV = os.path.join(OUTPUT_DIR, "unified_national_merged_data.csv")


def prepare_data():
    """Load and merge AirQuality Parquet and Thermal Power CSV using Pandas."""
    
    # 1. Load Air Quality Data (Parquet)
    print("=== 1. Load Air Quality Data (Merge Parquet for All Years) ===")
    
    # Search for all year folders
    all_year_folders = glob.glob(os.path.join(AIR_QUALITY_BASE_PATH, 'year_*'))
    
    if not all_year_folders:
        raise FileNotFoundError(f"Year folders (year_XXXX) not found in '{AIR_QUALITY_BASE_PATH}'.")

    # Find all .parquet files within each year folder
    all_parquet_files = []
    for folder in all_year_folders:
        all_parquet_files.extend(glob.glob(os.path.join(folder, '*.parquet')))
    
    if not all_parquet_files:
        raise FileNotFoundError("No Parquet files (*.parquet) found in year folders.")
    
    # Create DataFrame by reading list of Parquet files
    df_air = pd.read_parquet(all_parquet_files)
    print(f"-> Parquet files loaded: {len(all_parquet_files)}. Total {len(df_air)} rows of raw hourly data.")


    # 2. Preprocess Air Quality Data & Extract Date Info
    # Force extraction if year/month columns are missing or not loaded from partition metadata
    if 'year' not in df_air.columns:
        df_air['year'] = df_air['date_time'].astype(str).str.slice(0, 4).astype(int)
    if 'month' not in df_air.columns:
        df_air['month'] = df_air['date_time'].astype(str).str.slice(4, 6).astype(int)
    
    # Convert raw PM10 column to float (handling errors)
    df_air[PM10_RAW_COL] = pd.to_numeric(df_air[PM10_RAW_COL], errors='coerce') 
    
    
    # 3. Load Thermal Power Data & Unpivot (Wide -> Long)
    print("=== 2. Load Thermal Power Data & Unpivot (Wide -> Long) ===")
    df_power_wide = pd.read_csv(POWER_DATA_CSV)
    
    # Transform Wide Format to Long Format
    df_power_wide.rename(columns={df_power_wide.columns[0]: 'month'}, inplace=True)
    # Remove commas and convert to float for all data columns
    for col_name in df_power_wide.columns[1:]:
        df_power_wide[col_name] = df_power_wide[col_name].astype(str).str.replace(',', '').astype(float)
    
    df_power_long = df_power_wide.melt(id_vars=['month'], var_name='year', value_name='Power_GWh')
    df_power_long['year'] = df_power_long['year'].astype(int)
    df_power_long['month'] = df_power_long['month'].astype(int)
    
    
    # 4. Merge Data & Final Aggregation
    print("=== 3. Final Goal: National Monthly Aggregation (Create 264 rows) ===")
    
    # Merge Air Quality raw data with monthly Power data
    df_merged_raw = pd.merge(df_air, df_power_long, on=['year', 'month'], how='inner')
    
    # Final Aggregation: Calculate national average for PM10 and remove duplicates for Power_GWh
    df_national_monthly = df_merged_raw.groupby(['year', 'month']).agg(
        # Calculate national average of PM10
        national_avg_PM10=(PM10_RAW_COL, 'mean'), 
        # Take the first value for Power_GWh as it is already monthly aggregated
        Power_GWh=('Power_GWh', 'first') 
    ).reset_index()

    # Create Date Index
    df_national_monthly['Date'] = pd.to_datetime(df_national_monthly['year'].astype(str) + '-' + df_national_monthly['month'].astype(str) + '-01')
    df_national_monthly.set_index('Date', inplace=True)
    df_national_monthly.sort_index(inplace=True)
    
    print(f"-> Final aggregation complete. Total {len(df_national_monthly)} months of data (Target format).")

    return df_national_monthly

def save_merged_data(df):
    """Save merged DataFrame to a local CSV file."""
    
    # Create output directory
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Save as CSV
    df.to_csv(OUTPUT_LOCAL_MERGED_CSV, index=True, encoding='utf-8-sig')
    print(f"\n✅ Data integration and final aggregation complete! Merged file saved locally: {OUTPUT_LOCAL_MERGED_CSV}")


if __name__ == "__main__":
    try:
        final_merged_df = prepare_data()
        save_merged_data(final_merged_df)
    except Exception as e:
        print(f"\n❌ Execution Error: {e}")
        import traceback
        traceback.print_exc(file=sys.stdout)