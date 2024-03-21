import pandas as pd
import numpy as np
import productivity_configs as c
from databricks.connect import DatabricksSession

# Function to read in spark data and return pandas dataframe
def read_sql_data(spark, sandbox, folder, dataset):
    dataset_spark = spark.sql(f"SELECT * FROM {sandbox}.{folder}.{dataset}")
    dataset_pd = dataset_spark.toPandas()
    return dataset_pd


# Function to find closer milestones in order and return as a dictionary
def get_closer_milestone_order_dict(loan_df, milestone_df):
    # Get all closer milestones
    closer_milestones = milestone_df[(milestone_df[c.MILESTONE_OWNER] == c.CLOSER) & (~milestone_df[c.MILESTONE_ORDER].isin(c.INVALID_CLOSER_MILESTONE_ORDERS))]

    # Sort closer_milestones by Clientkey and MilestoneOrder
    sorted_closer_milestones = closer_milestones.sort_values(by=[c.CLIENT_KEY, c.MILESTONE_ORDER]).groupby(c.CLIENT_KEY)[c.MILESTONE_NAME].apply(list).reset_index()

    # Creating a dictionary to map Clientkey to a list of ordered milestone names
    closer_milestone_order_dict = pd.Series(sorted_closer_milestones[c.MILESTONE_NAME].values, index=sorted_closer_milestones[c.CLIENT_KEY]).to_dict()
    
    return closer_milestone_order_dict

# Updated function to get first non-missing date
def get_first_non_null_closer_date(row, closer_milestone_order_dict, closer_milestone_to_column):
    milestones = closer_milestone_order_dict.get(row[c.CLIENT_KEY], [])
    for milestone in milestones:
        # Retrieve the date column, returning None if the milestone is not found
        date_column = closer_milestone_to_column.get(milestone)
        # Check if date_column is not None and the value is not NA
        if date_column and pd.notna(row[date_column]):
            return row[date_column]
    return pd.NaT

#dsdata['CloserMilestoneDate'] = dsdata.apply(get_first_non_null_closer_date, axis=1)

# Function to return df with only the necessary columns
def filter_columns(df, columns):
    return df[columns]

# Function to replace 'NULL' strings with np.nan
def drop_nulls(df, column):
    # Drop NULLs from desired df, column
    df[column] = df[column].replace('NULL', np.nan)
    return df 

# TODO: Figure out which closing date to use for closers

# Create Year, Month, Day, and YMD columns
def create_ymd_cols(df, date_col, year=False, month=False, day=False, ymd=False, year_col='year', month_col='month', day_col='day', ymd_col='ymd'):
    # Create year,month, day, and/or YMD columns based on input
    if year:
        df[year_col] = pd.to_datetime(df[date_col], errors='coerce').dt.year
    if month:
        df[month_col] = pd.to_datetime(df[date_col], errors='coerce').dt.month
    if day:
        df[day_col] = pd.to_datetime(df[date_col], errors='coerce').dt.month
    if ymd:
        df[ymd_col] = pd.to_datetime(df[[year_col, month_col]].assign(day=1))
        df[ymd_col] = pd.to_datetime(df[ymd_col].dt.strftime('%Y-%m-%d'))
    return df

# Function to get aggregate counts
def get_counts(df, client_key, loan_number, employee_key, milestone_year, milestone_month, employee_count_column, loan_count_column):
    aggregate_df = df[
        (~df[employee_key].isna()) & (~df[milestone_year].isna()) & (~df[milestone_month].isna())
    ].groupby([client_key, milestone_year, milestone_month]).agg({
        employee_key: 'nunique',
        loan_number: 'nunique'
    }).reset_index().rename(columns={
        milestone_year: c.YEAR,
        milestone_month: c.MONTH,
        employee_key: employee_count_column,
        loan_number: loan_count_column
    })
    return aggregate_df

# Function to get productivity measure
def get_productivity(df, loan_count_column, employee_count_column, productivity_column):
    df[productivity_column] = df[loan_count_column] / df[employee_count_column]
    return df

# Join each aggregate table
def get_aggregate_productivity(processor_df, underwriter_df, closer_df):
    all_roles_df = processor_df.merge(underwriter_df, on=[c.CLIENT_KEY, c.YEAR, c.MONTH], how='outer')
    all_roles_df = all_roles_df.merge(closer_df, on=[c.CLIENT_KEY, c.YEAR, c.MONTH], how='outer')
    return all_roles_df

