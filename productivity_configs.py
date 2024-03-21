import pandas as pd
import numpy as np

# Data Tables
SANDBOX = 'datause1_sandbox'
FOLDER = 'nexus_vision'
DSDATA_UNIQUE_DATA = 'ds_unique_loan_record'
EMPLOYEE_DATA = 'ds_employee_2022_12_14'
DSMILESTONE_DATA = 'ds_milestone'

CLEAN_CLIENTS_LIST = [168, 192, 213, 218, 223, 245, 252, 255, 257, 258]

# DSMilestone Names
MILESTONE_ORDER = 'MilestoneOrder'
MILESTONE_NAME = 'MilestoneName'
MILESTONE_OWNER = 'MilestoneOwner'

# Closer Values
CLOSER = 'Closer'
CLEAR_TO_CLOSE_VALUE = 'Clear to Close'
SUBMITTED_TO_CLOSING_VALUE = 'Submitted to Closing'
CLOSING_DOCS_SENT_VALUE = 'Closing Docs Sent'
FUNDED_VALUE = 'Funded'
INVALID_CLOSER_MILESTONE_ORDERS = [0]

# Year and Month Columns
SUBMITTED_TO_PROCESSING_YEAR = 'SubmittoProc_Year'
SUBMITTED_TO_PROCESSING_MONTH = 'SubmittoProc_Month'
UNDERWRITER_SUBMISSION_YEAR = 'UWSubmission_Year'
UNDERWRITER_SUBMISSION_MONTH = 'UWSubmission_Month'
SUBMITTED_TO_CLOSER_YEAR = 'SubmittoCloser_Year'
SUBMITTED_TO_CLOSER_MONTH = 'SubmittoCloser_Month'
YEAR = 'Year'
MONTH = 'Month'

# Productivity Column Names
NUMBER_OF_PROCESSORS = 'NumberOfProcessors'
NUMBER_OF_LOANS_PROCESSED = 'NumberOfLoansProcessed'
PROCESSOR_PRODUCTIVITY = 'ProcessorProductivity'
NUMBER_OF_UNDERWRITERS = 'NumberOfUnderwriters'
NUMBER_OF_LOANS_UNDERWRITTEN = 'NumberOfLoansUnderwritten'
UNDERWRITER_PRODUCTIVITY = 'UnderwrtierProductivity'
NUMBER_OF_CLOSERS = 'NumberOfClosers'
NUMBER_OF_LOANS_CLOSED = 'NumberOfLoansClosed'
CLOSER_PRODUCTIVITY = 'CloserProductivity'

# DSData Names
CLIENT_KEY = 'Clientkey'
APPLICATION_KEY = 'Applicationkey'
CLIENT_NAME = 'ClientName'
LOAN_NUMBER = 'LoanNumber'
APPLICATION_DATE = 'ApplicationDate'
SUBMITTED_TO_PROCESSING_DATE = 'SubmittoProc'
UNDERWRITER_SUBMISSION_DATE = 'UWSubmission'
CLIENT_SPECIFIC_CLOSER_INDICATOR_DATE = 'ClientSpecificCloserIndicatorDate'
CONDITIONAL_APPROVAL_DATE = 'CondApproval'
SUSPENSION_DATE = 'Suspension'
FINAL_APPROVAL_DATE = 'FinalApproval'
CLEAR_TO_CLOSE_DATE = 'ClearToClose'
# SUBMITTED_TO_CLOSING_DATE = 'SubmittedToClosing'
# CLOSING_DOCS_SENT_DATE = 'ClosingDocsSent'
FUNDED_DATE = 'Funded'
SOLD_TO_INVESTOR_DATE = 'SoldToInvestor'
BUSINESS_DAYS_APPLICATION_TO_FUNDED = 'BusinessDaysAppToFund'
BUSINESS_DAYS_APPLICATION_TO_UNDERWRITER_SUBMISSION = 'BusinessDaysApptoUWSub'
BUSINESS_DAYS_APPLICATION_TO_FINAL_APPROVAL = 'BusinessDaysApptoFinalApproval'
BUSINESS_DAYS_APPLICATION_TO_CLEAR_TO_CLOSE = 'BusinessDaysApptoClearToClose'
BUSINESS_DAYS_APPLICATION_TO_PROCESSING = 'BusinessDaysAppToProc'
CURRENT_MILESTONE_KEY = 'CurrentMilestonekey'
CURRENT_MILESTONE = 'CurrentMilestone'
CURRENT_MILESTONE_DATE = 'CurrentMilestoneDate'
LOCK_DATE = 'LockDate'
EXPIRATION_DATE = 'Expiration'
LOCK_STATUS = 'LockStatus'
LOAN_AMOUNT = 'LoanAmount'
LOAN_TYPE = 'LoanType'
LOAN_CATEGORY = 'LoanCategory'
PRODUCT = 'Product'
LOAN_TO_VALUE = 'LTV'
COMBINED_LOAN_TO_VALUE = 'CLTV'
LIEN_POSITION = 'LienPosition'
PURPOSE = 'Purpose'
REFINANCE_PURPOSE = 'RefiPurpose'
AMORTIZATION_TYPE = 'AmortType'
OCCUPANCY_TYPE = 'OccupancyType'
APPRAISAL_ORDER_DATE = 'AppraisalOrder'
APPRAISAL_RECEIVED_DATE = 'AppraisalRcv'
TITLE_ORDER_DATE = 'TitleOrder'
TITLE_RECEIVED_DATE = 'TitleRcv'
APPRAISAL_VALUE = 'Apprval'
INTEREST_RATE = 'IntRate'
PRINCIPAL_AND_INTEREST_AMOUNT = 'PNIPmt'
PRINCIPAL_INTEREST_TAXES_AND_INSURANCE_AMOUNT = 'PITIPmt'
CREDIT_SCORE = 'CreditScore'
HOUSING_TO_INCOME_RATIO = 'HTI'
DEBT_TO_INCOME_RATIO = 'DTI'
GROSS_INCOME = 'GrossIncome'
LIQUID_ASSETS = 'LiquidAssets'
CASH_FROM_BORROWER = 'CashfromBorrower'
SELF_EMPLOYED = 'SelfEmployed'
ESTIMATED_CLOSING_DATE = 'EstClosingDate'
UNDERWRITER_CONDITIONS = 'UWCondCR'
UNDERWRITER_TOUCHES = 'UWTouches'
LAST_LOAN_ORIGINATING_SYSTEM_CHANGE_DATE = 'LastLOSChangeDt'
LOAN_OFFICER_EMPLOYEE_KEY = 'LoanOfficerEmployeeKey'
LOAN_OFFICER_ASSISTANT_EMPLOYEE_KEY = 'LoanOfficerAsstEmployeeKey'
PROCESSOR_EMPLOYEE_KEY = 'ProcessorEmployeeKey'
UNDERWRITER_EMPLOYEE_KEY = 'UnderwriterEmployeeKey'
CLOSER_EMPLOYEE_KEY = 'CloserEmployeeKey'
DOWNLOAD_UTC_DATE = 'DownLoadUTCDate'
LOAN_TERM = 'LoanTerm'
DEACTIVATED = 'Deactivated'
DATA_SOURCE = 'DataSource'

# Closer MilestoneName Mapping
closer_milestone_to_column = {
    CLEAR_TO_CLOSE_VALUE: CLEAR_TO_CLOSE_DATE,
 #   SUBMITTED_TO_CLOSING_VALUE: SUBMITTED_TO_CLOSING_DATE,
 #   CLOSING_DOCS_SENT_VALUE: CLOSING_DOCS_SENT_DATE,
    FUNDED_VALUE: FUNDED_DATE
}

# Productivity Columns
PRODUCTIVITY_COLUMNS = [CLIENT_KEY, LOAN_NUMBER, # Appliction identifiers
                        PROCESSOR_EMPLOYEE_KEY, UNDERWRITER_EMPLOYEE_KEY, CLOSER_EMPLOYEE_KEY, # Employee identifiers
                        SUBMITTED_TO_PROCESSING_DATE, UNDERWRITER_SUBMISSION_DATE, CLIENT_SPECIFIC_CLOSER_INDICATOR_DATE] # Milestone dates