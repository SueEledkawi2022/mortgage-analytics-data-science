import pandas as pd
import numpy as np
import configs as c
import preprocess_data as prep
from functools import reduce


def create_interest_rate_range_col(df, col):
    # Create IntRateRange Conditions..
    conditions = [
    df[col] < 3,
    (df[col] >= 3) & (df[col] < 4),
    (df[col] >= 4) & (df[col] < 5),
    (df[col] >= 5) & (df[col] < 6),
    (df[col] >= 6) & (df[col] < 7),
    df[col] >= 7
    ]
    # Assign the ranges to 'IntRateRanges' based on conditions using numpy.select()
    df['IntRateRanges'] = np.select(conditions, c.RANGES, default='Unknown')
    return df 


def groupby_for_loans_amount_and_volume(df):
    ## groupby to extract loan volume per client by month-year
    total_loan_volume_by_month_year = df.groupby(c.VOLUME_GROUPBY_COLUMNS)['Applicationkey'].nunique().reset_index(name='TotalLoans_my')

    ## groupby to extract loan amount per client by month-year
    unique_loan_amounts = df.groupby(c.LOAN_AMOUNT_GROUPBY_COLUMNS)['LoanAmount'].last().reset_index()
    total_loan_amount_by_month_year = unique_loan_amounts.groupby(c.LOAN_AMOUNT_GROUPBY_COLUMNS)['LoanAmount'].sum().reset_index(name='TotalLoanAmount_my')

    ## create dataframe that outputs each client, along with their loan metrics..
    loan_totals_by_month_year_ = [total_loan_volume_by_month_year, total_loan_amount_by_month_year]
    loan_totals_by_month_year = reduce(lambda  left, right: pd.merge(left, right, on=c.VOLUME_GROUPBY_COLUMNS, how='left'), loan_totals_by_month_year_)
    return loan_totals_by_month_year


def calculate_percent_change(group):
    ## clacluate percent change for LoanAmount and LoanVolume for correlation calculation..
    group['percent_change_TotalLoans'] = group['TotalLoans_my'].pct_change()*100
    group['percent_change_TotalLoanAmount'] = group['TotalLoanAmount_my'].pct_change()*100
    return group


def calulate_and_analyse_loan_volume_amount_correlations(df):
    ## Function purpose: calculates correlation between loan dollar amount and loan volume w.r.t. both's unit change (percent increase/decrease)
    ## Function output: a message indicating if we have a high positive, negative, or neutral (below 0.7) correlation between the two variables

    ## Groupby and get Loan Amount and Volume totals
    loan_totals_by_month_year = groupby_for_loans_amount_and_volume(df)

    # Calculate percent-change of Loan Amount and Loan Volume and compare correlation..
    loan_totals_by_month_year = loan_totals_by_month_year.groupby('Clientkey').apply(calculate_percent_change)
    loan_totals_by_month_year_last_year_data = prep.subset_data(loan_totals_by_month_year, only_funded_loans=False, only_clean_clients=False, most_recent_year_data=True)

    ## create correlation dataframes 
    loan_totals_by_month_year_corr = loan_totals_by_month_year.corr()
    # sub to 2023 data since that's what we'll use for forecasts...
    loan_totals_by_month_year_last_year_data_corr = loan_totals_by_month_year_last_year_data.corr()


    # Extract correlation value between the specified columns, for all data + most recent also
    correlation_value_all_years = loan_totals_by_month_year_corr.loc['percent_change_LoanVolume', 'percent_change_TotalLoanAmount']
    correlation_value_last_year = loan_totals_by_month_year_last_year_data_corr.loc['percent_change_LoanVolume', 'percent_change_TotalLoanAmount']


    # Determine the nature of the correlation for both correlations (all years + last year)
    ## all years
    if correlation_value_all_years > 0.7:
        correlation_type = "high positive"
    elif correlation_value_all_years < 0:
        correlation_type = "negative"
    else:
        correlation_type = "neutral"

    # last year
    if correlation_value_last_year > 0.7:
        correlation_type_last_year = "high positive"
    elif correlation_value_last_year < 0:
        correlation_type_last_year = "negative"
    else:
        correlation_type_last_year = "neutral"

    # Provide analysis message
    analysis_message = f"Correlations between the percent change of the total loans' dollar amount and percent change of loan volume are {correlation_value_all_years:.3f}, and {correlation_type}.\nLast year's correlations between the two variables are {correlation_value_last_year:.3f}, and {correlation_type_last_year}."

    return print(analysis_message)


def calculate_cumulative_quarters(group):
    ## Get the cum-sum at the end of each quarter, to apply quarterly forecasts later...

    # Sort the group by 'YMD' before calculating cumulative sum
    group = group.sort_values('YMD')
    
    # Calculate cumulative sum for LoanVolume_my and TotalLoanAmount_my, resetting at the start of each quarter
    group['LoanVolume_my_Q'] = group.groupby(group['YMD'].dt.to_period("Q")).cumsum()['LoanVolume_my']
    group['TotalLoanAmount_my_Q'] = group.groupby(group['YMD'].dt.to_period("Q")).cumsum()['TotalLoanAmount_my']
    
    return group