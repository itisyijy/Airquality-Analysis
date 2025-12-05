# -*- coding: utf-8 -*-

import pandas as pd
from scipy.stats import pearsonr # For calculating Pearson correlation coefficient
import os
import matplotlib.pyplot as plt
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
PREDICTOR = 'Power_GWh'                  # Predictor Variable

# --- Result File Configuration ---
OUTPUT_LOCAL_LAG_CORR = os.path.join(OUTPUT_DIR, "lagged_correlation_results_pm10.csv")


def analyze_lag_correlation():
    
    # 1. Load Unified CSV Data
    print("=== 1. Start Loading Unified CSV Data ===")
    try:
        # The unified CSV file was saved with a Date index, so use index_col=0 and parse_dates=True
        df_pandas = pd.read_csv(UNIFIED_MERGED_CSV, index_col=0, parse_dates=True)
        df_pandas.sort_index(inplace=True)
        print(f"-> Data loaded successfully. Total {len(df_pandas)} months of data.")
        
    except FileNotFoundError:
        print(f"❌ Error: Unified data file ({UNIFIED_MERGED_CSV}) not found.")
        print("      Please run 'unified_analysis_parquet.py' first to generate this file.")
        return

    # 2. Configure Analysis Columns and Validate
    df_analysis = df_pandas[[TARGET_POLLUTANT, PREDICTOR]].astype(float) 
    
    lag_results = []
    
    # 3. Calculate Lagged Correlation
    # Analyze Lag from 1 to 6 months
    print("\n=== 2. Start Lagged Correlation Analysis of Power Generation (Power) ===")
    
    for lag in range(1, 7):
        # Create Lag Column: Shift power generation data down by 'lag' period
        lag_col_name = f'{PREDICTOR}_Lag_{lag}'
        df_analysis[lag_col_name] = df_analysis[PREDICTOR].shift(lag)
        
        # Prepare arrays by selecting only valid values without NaNs
        valid_data = df_analysis[[TARGET_POLLUTANT, lag_col_name]].dropna()
        
        if len(valid_data) > 5: # Analysis requires at least 5 data points
            correlation, p_value = pearsonr(
                valid_data[TARGET_POLLUTANT],
                valid_data[lag_col_name]
            )
            
            lag_results.append({
                'Lag': lag,
                'Correlation_Coefficient': correlation,
                'P_Value': p_value
            })
            
            print(f"  - Lag {lag} month(s): Correlation = {correlation:.4f}, P-value = {p_value:.4f}")
        else:
            print(f"  - Lag {lag} month(s): Insufficient data points for analysis.")


    # 4. Organize and Save Results
    df_lag_results = pd.DataFrame(lag_results)
    
    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    # Save results to a local CSV file
    df_lag_results.to_csv(OUTPUT_LOCAL_LAG_CORR, index=False)
    
    print(f"\n✅ Analysis Complete. Result CSV saved locally: {OUTPUT_LOCAL_LAG_CORR}")
    
    # Visually output the Optimal Lag
    if not df_lag_results.empty:
        best_lag = df_lag_results.loc[df_lag_results['Correlation_Coefficient'].abs().idxmax()]
        print(f"\n💡 [Optimal Lag Analysis Result]: The time delay with the highest correlation coefficient is {int(best_lag['Lag'])} month(s) (Correlation: {best_lag['Correlation_Coefficient']:.4f}).")


if __name__ == "__main__":
    analyze_lag_correlation()