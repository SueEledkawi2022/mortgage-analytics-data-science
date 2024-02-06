import pandas as pd
import pyspark.pandas as ps
import numpy as np
# nb path
import configs as c


def read_sql_data(sandbox, folder, dataset):
    dataset_spark = spark.sql(f"SELECT * FROM {c.SANDBOX}.{c.FOLDER}.{c.DATASET1}")
    dataset_pd = dataset_spark.toPandas()
    return dataset_pd

def create_mba_forecast_df(dollar_amount_list, quarters):
    # Create DF
    MBA_data = pd.DataFrame({
        'BillionDollars': dollar_amount_list,
        'Quarter': quarters
    })

    # Extract years and quarters from the input quarters list
    years = ['20' + q.split('_')[1] for q in quarters]  
    quarters = [q.split('_')[0] for q in quarters]  

    # Mapping the quarter strings to specific months
    quarter_to_month = {'Q1': '03', 'Q2': '06', 'Q3': '09', 'Q4': '12'}
    months = [quarter_to_month[q] for q in quarters]  # Last month of each quarter

    # Creating a new column with formatted dates
    MBA_data['FormattedQuarter'] = pd.to_datetime([f"{y}-{m}" for y, m in zip(years, months)])
    MBA_data.set_index('FormattedQuarter', inplace=True)

    # Normalize the billion-dollar amount to percentages
    MBA_data['NormalizedForecast'] = MBA_data['BillionDollars'].pct_change() * 100
    MBA_data = MBA_data.reset_index()

    # Let's only grab the columns we care about from MBA data
    MBA_forecast = MBA_data[['Quarter','FormattedQuarter', 'NormalizedForecast']]
    MBA_forecast = MBA_forecast.rename(columns={'FormattedQuarter':'YMD'})

    return MBA_forecast