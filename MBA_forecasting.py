import pandas as pd 
import numpy as np 
import create_metrics as cm 
import preprocess_data as p
import configs as c

def prepare_data_for_forecast(funded_clean_clients_loans_dsdata):
    ## Format date-YMD column
    funded_clean_clients_loans_totals = cm.groupby_for_loans_amount_and_volume(funded_clean_clients_loans_dsdata)
    funded_clean_clients_loans_totals = p.create_ymd_cols(funded_clean_clients_loans_totals, _, year=False, month=False, day=False, ymd=True)

    ## Subset the data we will apply forecast to - ONLY most recent year data
    funded_clean_clients_loans_totals_recent_year = p.subset_data(funded_clean_clients_loans_totals, only_funded_loans=False, only_clean_clients=False, most_recent_year_data=True)
    # Subset to only colmns we care about #TODO: verify desired output is NOT full df
    funded_clean_clients_loans_totals_recent_year = funded_clean_clients_loans_totals_recent_year[['YMD', 'Clientkey','TotalLoanAmount_my', 'LoanVolume_my']]


    df_to_forecast = funded_clean_clients_loans_totals_recent_year.groupby('Clientkey').apply(cm.calculate_cumulative_quarters).reset_index(drop=True)

    return df_to_forecast


def apply_mba_forecast(df_to_forecast, MBA_data, last_desired_forecast_date):
    
    ## Filter MBA forecast data for the relevant time period
    MBA_forecast = MBA_data[MBA_data['YMD'] >= pd.to_datetime(last_desired_forecast_date)] #'2023-09-01'

    # Initialize empty DataFrames to store forecast results
    forecasted_dataframes = []

    # Group by Clientkey and apply forecast for each group
    for clientkey, group in df_to_forecast.groupby('Clientkey'):
        # Merge the MBA forecast data with the loan totals data on 'YMD' column
        merged_data = pd.merge(group, MBA_forecast, on='YMD', how='right')
        merged_data['Clientkey'] = merged_data['Clientkey'].iloc[0]
        
        # Initialize LoanVolume Forecast column
        merged_data['LoanVolume_Forecast'] = 0
        # Set initial value based on Q data
        merged_data['LoanVolume_Forecast'].iloc[0] = merged_data['LoanVolume_my_Q'].iloc[0]

        # Apply LoanVolume Forecast from MBA
        for i in range(1, len(merged_data)):
            merged_data['LoanVolume_Forecast'].iloc[i] = merged_data['LoanVolume_Forecast'].iloc[i-1] * (1 + merged_data['NormalizedForecast'].iloc[i]/100)

        # Initialize LoanAmount Forecast column
        merged_data['LoanAmount_Forecast'] = 0
        # Set initial value based on Q data
        merged_data['LoanAmount_Forecast'].iloc[0] = merged_data['TotalLoanAmount_my_Q'].iloc[0]

        # Apply LoanAmount Forecast from MBA
        for i in range(1, len(merged_data)):
            if not pd.isnull(merged_data['LoanAmount_Forecast'].iloc[i-1]):
                merged_data['LoanAmount_Forecast'].iloc[i] = merged_data['LoanAmount_Forecast'].iloc[i-1] * (1 + merged_data['NormalizedForecast'].iloc[i]/100)

        # # Translate into monthly data 
        merged_data['LoanAmountForecast_m'] = np.round(merged_data['LoanAmount_Forecast'] / 3, 4)
        merged_data['LoanVolumeForecast_m'] = np.round(merged_data['LoanVolume_Forecast'] / 3, 4)
        
        # Append the forecasted dataframe for the current Clientkey to the list
        forecasted_dataframes.append(merged_data)
    
    # Concatenate all forecasted dataframes
    forecasted_data = pd.concat(forecasted_dataframes)
    
    return forecasted_data


