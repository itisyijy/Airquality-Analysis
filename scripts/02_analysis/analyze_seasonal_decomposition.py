# -*- coding: utf-8 -*-

import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose # Library for seasonal decomposition
import matplotlib.pyplot as plt
import os
import seaborn as sns
import glob
import sys

# --- System Path Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
PROJECT_ROOT_DIR = os.path.join(SCRIPT_DIR, '..', '..')

# --- Input/Output Directory Configuration ---
OUTPUT_DIR = os.path.join(PROJECT_ROOT_DIR, 'results', 'pandas_analysis')
UNIFIED_MERGED_CSV = os.path.join(OUTPUT_DIR, "unified_national_merged_data.csv")

# --- Analysis Target Configuration ---
TARGET_POLLUTANT = 'national_avg_PM10'   # Dependent Variable
TARGET_POWER = 'Power_GWh'               # Independent Variable

# --- Result File Configuration ---
OUTPUT_LOCAL_DECOMPOSE_PM10 = os.path.join(OUTPUT_DIR, "seasonal_decomposition_pm10.png")
OUTPUT_LOCAL_DECOMPOSE_POWER = os.path.join(OUTPUT_DIR, "seasonal_decomposition_power.png")


def analyze_decomposition():
    
    # 1. Load Unified CSV Data
    print("=== 1. Start Loading Unified CSV Data & Preparing Time Series ===")
    try:
        # The unified CSV file has a Date index, so use index_col=0 and parse_dates=True
        df_pandas = pd.read_csv(UNIFIED_MERGED_CSV, index_col=0, parse_dates=True)
        df_pandas.sort_index(inplace=True)
        print(f"-> Data loaded successfully. Total {len(df_pandas)} months of data.")
        
    except FileNotFoundError:
        print(f"❌ Error: Unified data file ({UNIFIED_MERGED_CSV}) not found.")
        print("      Please run 'unified_analysis_parquet.py' first to generate this file.")
        return

    # 2. Configure Analysis Columns and Validate
    df_analysis = df_pandas[[TARGET_POLLUTANT, TARGET_POWER]].astype(float)
    
    # 3. Decompose PM10 Concentration Time Series (Using Additive Model)
    print("\n=== 2. Start Seasonal Decomposition of PM10 ===")
    # Additive Model: Observation = Trend + Seasonality + Residual
    # period=12: The data is monthly, so the period is 12 months
    result_pm10 = seasonal_decompose(df_analysis[TARGET_POLLUTANT].dropna(), model='additive', period=12)
    
    # 4. Decompose Thermal Power Generation Time Series
    print("=== 3. Start Seasonal Decomposition of Thermal Power ===")
    result_power = seasonal_decompose(df_analysis[TARGET_POWER].dropna(), model='additive', period=12)
    
    # 5. Visualization 1: PM10 Decomposition Results
    print(f"\n=== 4. Visualization: Saving PM10 Decomposition Results ({OUTPUT_LOCAL_DECOMPOSE_PM10}) ===")
    fig, axes = plt.subplots(4, 1, figsize=(14, 10), sharex=True)
    
    axes[0].plot(result_pm10.observed, label='Observed Data')
    axes[0].set_title('Decomposition of National Average PM10')
    axes[0].legend(loc='upper left')
    
    axes[1].plot(result_pm10.trend, label='Trend', color='red')
    axes[1].legend(loc='upper left')
    
    axes[2].plot(result_pm10.seasonal, label='Seasonality', color='green')
    axes[2].legend(loc='upper left')
    
    axes[3].plot(result_pm10.resid, label='Residuals', color='gray')
    axes[3].legend(loc='upper left')
    
    plt.xlabel("Date")
    plt.tight_layout()
    plt.savefig(OUTPUT_LOCAL_DECOMPOSE_PM10)
    
    # 6. Visualization 2: Power Generation Decomposition Results
    print(f"=== 5. Visualization: Saving Power Generation Decomposition Results ({OUTPUT_LOCAL_DECOMPOSE_POWER}) ===")
    fig, axes = plt.subplots(4, 1, figsize=(14, 10), sharex=True)
    
    axes[0].plot(result_power.observed, label='Observed Data')
    axes[0].set_title('Decomposition of National Thermal Power Generation')
    axes[0].legend(loc='upper left')
    
    axes[1].plot(result_power.trend, label='Trend', color='red')
    axes[1].legend(loc='upper left')
    
    axes[2].plot(result_power.seasonal, label='Seasonality', color='green')
    axes[2].legend(loc='upper left')
    
    axes[3].plot(result_power.resid, label='Residuals', color='gray')
    axes[3].legend(loc='upper left')
    
    plt.xlabel("Date")
    plt.tight_layout()
    plt.savefig(OUTPUT_LOCAL_DECOMPOSE_POWER)
    
    print("\n✅ Seasonal decomposition analysis complete. Result images saved locally.")
    
if __name__ == "__main__":
    analyze_decomposition()