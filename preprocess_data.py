import pandas as pd
import configs as c


def convert_col_types(df):
    # To convert column types as necessary...
    for col in df.columns:
        if col in c.INT_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(000).astype(int)
        elif col in c.FLOAT_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(000).astype(float)
        elif col in c.DATETIME_COLUMNS:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df 


def drop_nulls(df, col):
    # Drop NULLs from desired df, column
    df = df[df[col]!='NULL']
    return df 


def create_ymd_cols(df, date_col, year=False, month=False, day=False, ymd=False):
    # Create year,month, day, and/or YMD columns based on input
    if year:
        df[c.YEAR] = df[date_col].dt.year
    if month:
        df[c.MONTH] = df[date_col].dt.month
    if day:
        df[c.DAY] = df[date_col].dt.month
    if ymd:
        df[c.YMD] = pd.to_datetime(df[[c.YEAR, c.MONTH]].assign(day=1))
        df[c.YMD] = pd.to_datetime(df[c.YMD].dt.strftime('%Y-%m-%d'))
    return df


def subset_data(df, only_funded_loans=False, only_clean_clients=False, most_recent_year_data=False):
    ## Subset data as desired..
    if only_funded_loans:
        ## Subsetting: to ONLY funded loans
        df = df[df[c.FUNDED].notnull()]

    if only_clean_clients:
        ## Subesetting: to df_funded to ONLY clean clients -> provided Margie...change as desired 
        df = df[df[c.CLIENT_KEY].isin(c.CLEAN_CLIENTS_LIST)]

    #TODO: verify this is the best way to do this...i.e. what if .max() gives 2024 
    if most_recent_year_data:
        ## Subsetting: to most recent year data
        df = df[df[c.YEAR] == df[c.YEAR].unique().max()]

    return df

