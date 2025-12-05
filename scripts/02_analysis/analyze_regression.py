# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import statsmodels.api as sm # Library for regression analysis
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

# --- Analysis Model Configuration ---
TARGET_POLLUTANT = 'national_avg_PM10'  # Dependent Variable
OPTIMAL_LAG = 4                         # Independent Variable: 4-month lag (Derived from Lag Analysis)

# --- Result File Configuration ---
OUTPUT_LOCAL_SUMMARY = os.path.join(OUTPUT_DIR, "regression_summary_PM10_final.txt")
OUTPUT_LOCAL_HEATMAP = os.path.join(OUTPUT_DIR, "correlation_heatmap_pm10.png")


def analyze_regression():
    
    # 1. Load Unified CSV Data
    print("=== 1. Start Loading Unified CSV Data ===")
    try:
        # The unified CSV file has a Date index, so use index_col=0 and parse_dates=True
        df_pandas = pd.read_csv(UNIFIED_MERGED_CSV, index_col=0, parse_dates=True)
        df_pandas.sort_index(inplace=True)
        print(f"-> Data loaded successfully. Total {len(df_pandas)} months of time-series data.")
        
    except FileNotFoundError:
        print(f"❌ Error: Unified data file ({UNIFIED_MERGED_CSV}) not found.")
        print("      Please run 'unified_analysis_parquet.py' first to generate this file.")
        return
    
    # 2. Multiple Regression Modeling
    print("\n=== 2. Setup and Fit Multiple Regression Model ===")
    
    # --- Create Key Lag Variable ---
    df = df_pandas.copy() # Preserve original data
    lag_col_name = f'Power_GWh_Lag{OPTIMAL_LAG}'
    df[lag_col_name] = df['Power_GWh'].shift(OPTIMAL_LAG)
    
    # --- Create Control Variables ---
    df['month'] = df.index.month
    
    # Create Month dummy variables to control for seasonality
    month_dummies = pd.get_dummies(df['month'], prefix='Month', drop_first=True) # Use January as Reference
    df = pd.concat([df, month_dummies], axis=1)

    # Create Long-term Trend variable
    df['Trend'] = np.arange(len(df))
    
    # Remove NaN rows caused by lagging and rows unnecessary for analysis
    df_regress = df.dropna()

    # Dependent Variable (Y): PM10
    Y = df_regress[TARGET_POLLUTANT]
    
    # Independent Variables (X): Lagged Power, Trend, Month Dummies
    X_vars = [lag_col_name, 'Trend'] + [c for c in df_regress.columns if c.startswith('Month_')]
    
    X = df_regress[X_vars]
    
    # Explicitly convert all variables to Float type to prevent Statsmodels ValueError.
    Y = Y.astype(float)
    X = X.astype(float)

    X = sm.add_constant(X) # Add Constant (Intercept)

    # Fit OLS (Ordinary Least Squares) Model
    model = sm.OLS(Y, X).fit()

    # 3. Print and Save Results
    print("\n=== 3. Regression Analysis Summary ===")
    summary_text = model.summary().as_text()
    
    print(summary_text)

    # Save results to a local file
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    with open(OUTPUT_LOCAL_SUMMARY, 'w', encoding='utf-8') as f:
        f.write(summary_text)
        
    print(f"\n✅ Final analysis complete. Summary result saved locally: {OUTPUT_LOCAL_SUMMARY}")
    
if __name__ == "__main__":
    analyze_regression()