### Variables for: read_data
## read_data.read_sql_data
SANDBOX = "datause1_sandbox"
FOLDER = "nexus_vision"
DATASET1 = "ds_unique_loan_record"
DATASET2 = "ds_data_and_ds_data_prior"


### Variables for: preprocess_data
## preprocess_data.convert_col_types 
INT_COLUMNS = [
        'IDKey', 'Clientkey', 'Applicationkey', 'BusinessDaysAppToFund', 'BusinessDaysApptoUWSub',
        'BusinessDaysApptoFinalApproval', 'BusinessDaysApptoClearToClose', 'BusinessDaysAppToProc',
        'CreditScore','CurrentMilestonekey', 'SelfEmployed','UWCondCR','UWTouches','LoanOfficerEmployeeKey',
        'LoanOfficerAsstEmployeeKey','ProcessorEmployeeKey','UnderwriterEmployeeKey','CloserEmployeeKey'
        ]
FLOAT_COLUMNS = [
        'LoanAmount','LTV','CLTV','Apprval','IntRate','PNIPmt','PITIPmt','HTI','DTI','GrossIncome',
        'LiquidAssets','CashfromBorrower'
        ]
    
DATETIME_COLUMNS = [
        'ApplicationDate', 'SubmittoProc', 'UWSubmission', 'FinalApproval', 'Funded', 'FormattedQuarter', 'YMD'
        ]

## preprocess_data.create_ymd_cols 
YEAR = 'year'
MONTH = 'month'
DAY = 'day'
YMD = 'YMD'

## preprocess_data.subset_data
CLEAN_CLIENTS_LIST = [168, 192, 213, 218, 223, 245, 252, 255, 257, 258]


### Variables for: create_metrics
## create_metrics.groupby_for_loans_amount_and_volume
VOLUME_GROUPBY_COLUMNS = ['Clientkey','year', 'month']
LOAN_AMOUNT_GROUPBY_COLUMNS = ['Clientkey','year', 'month','Applicationkey']

## create_metrics.create_interest_rate_range_col
RANGES = ['<3', '>=3', '>=4', '>=5', '>=6', '>=7']


## nb configs ... do i need to update defined GROUPBY_COLUMNS, col-list above? 
APPLICATION_KEY = 'Applicationkey'
FUNDED = 'Funded'

LOAN_AMOUNT = 'LoanAmount'
CLIENT_KEY = 'Clientkey'