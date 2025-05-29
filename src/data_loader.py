import pandas as pd

def load_and_merge_data(
    fao_path="../data/raw/food_price_indices_data_may25.csv",
    wb_path="../data/raw/CMO-Historical-Data-Monthly.xlsx",
    oecd_path="../data/raw/OECD_CPI.csv",
    output_path="../data/processed/merged_clean_data.csv"
):
    # Loading FAO Food Price Index.
    fao_df = pd.read_csv(fao_path)
    if {'Year', 'Month'}.issubset(fao_df.columns):
        fao_df['date'] = pd.to_datetime(fao_df[['Year', 'Month']].astype(str).agg('-'.join, axis=1))
    elif 'Date' in fao_df.columns:
        fao_df['date'] = pd.to_datetime(fao_df['Date'])
    else:
        raise ValueError("FAO data must contain 'Year' and 'Month' or 'Date' column")
    fao_df = fao_df.drop(columns=['Year', 'Month', 'Date'], errors='ignore')

    # Loading World Bank Commodity Prices.
    wb_df = pd.read_excel(wb_path, sheet_name="Monthly Prices", skiprows=4)
    wb_df.rename(columns={wb_df.columns[0]: 'Date'}, inplace=True)
    wb_df['date'] = pd.to_datetime(wb_df['Date'])
    wb_df = wb_df.drop(columns=['Date'])

    # Loading OECD CPI data.
    oecd_df = pd.read_csv(oecd_path)
    oecd_df = oecd_df[oecd_df['MEASURE'] == 'GY']
    oecd_df = oecd_df[oecd_df['LOCATION'] == 'OECD']
    oecd_df['date'] = pd.to_datetime(oecd_df['TIME'])
    oecd_df = oecd_df[['date', 'Value']].rename(columns={'Value': 'CPI'})

    # Merging datasets.
    merged = pd.merge(fao_df, wb_df, on='date', how='inner')
    merged = pd.merge(merged, oecd_df, on='date', how='left')

    # Saving the merged dataset.
    merged.to_csv(output_path, index=False)
    return merged
