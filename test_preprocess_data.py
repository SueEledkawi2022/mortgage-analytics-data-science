import pandas as pd
from functools import reduce
import configs as c
import preprocess_data as prep
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np


def test_convert_col_types():

    # Test integer columns
    df = pd.DataFrame({'IDKey': ['1','2','3'], 'OtherCol': [4,5,6]})
    expected = pd.DataFrame({'IDKey': [1,2,3], 'OtherCol': [4,5,6]})
    actual = prep.convert_col_types(df)
    assert_frame_equal(actual, expected)

    # Test float columns
    df = pd.DataFrame({'LoanAmount': ['1.5','2.5','3.5'], 'OtherCol': [4,5,6]})
    expected = pd.DataFrame({'LoanAmount': [1.5,2.5,3.5], 'OtherCol': [4,5,6]})
    actual = prep.convert_col_types(df)
    assert_frame_equal(actual, expected)

    # Test datetime columns
    df = pd.DataFrame({'ApplicationDate': ['2020-01-01','2020-02-01','2020-03-01'], 'OtherCol': [4,5,6]})
    expected = pd.DataFrame({'ApplicationDate': pd.to_datetime(['2020-01-01','2020-02-01','2020-03-01']), 'OtherCol': [4,5,6]})
    actual = prep.convert_col_types(df)
    assert_frame_equal(actual, expected)

    # Test no change for other columns
    df = pd.DataFrame({'OtherCol': ['a','b','c']})
    expected = pd.DataFrame({'OtherCol': ['a','b','c']})
    actual = prep.convert_col_types(df)
    assert_frame_equal(actual, expected)


def test_create_ymd_cols():
    df = pd.DataFrame({'date': pd.date_range('2020-01-01', periods=5)})

    expected = pd.DataFrame({
        'date': pd.date_range('2020-01-01', periods=5),
        'year': [2020, 2020, 2020, 2020, 2020],
        'month': [1, 1, 1, 1, 1],
        'day': [1, 1, 1, 1, 1],
        'ymd': pd.to_datetime(['2020-01-01', '2020-01-01', '2020-01-01', '2020-01-01', '2020-01-01'])
    })

    result = prep.create_ymd_cols(df, 'date', year=True, month=True, day=True, ymd=True)

    assert_frame_equal(result, expected)


def test_subset_data():
    # Create sample input dataframe
    df = pd.DataFrame({'FUNDED': [1, np.nan, 2, 3],
                       'CLIENT_KEY': [1, 2, 3, 4],
                       'YEAR': [2020, 2021, 2021, 2022],
                       'VALUE': [10, 20, 30, 40]})

    # Expected output for funded loans only
    exp_funded = pd.DataFrame({'FUNDED': [1, 2, 3],
                               'CLIENT_KEY': [1, 3, 4],
                               'YEAR': [2020, 2021, 2022],
                               'VALUE': [10, 30, 40]})

    # Expected output for clean clients only
    exp_clean = pd.DataFrame({'FUNDED': [np.nan, 2],
                              'CLIENT_KEY': [2, 3],
                              'YEAR': [2021, 2021],
                              'VALUE': [20, 30]})

    # Expected output for most recent year only
    exp_recent = pd.DataFrame({'FUNDED': [3],
                               'CLIENT_KEY': [4],
                               'YEAR': [2022],
                               'VALUE': [40]})

    # Test funded loans only
    result = prep.subset_data(df, only_funded_loans=True)
    assert_frame_equal(result, exp_funded)

    # Test clean clients only
    result = prep.subset_data(df, only_clean_clients=True)
    assert_frame_equal(result, exp_clean)

    # Test most recent year only
    result = prep.subset_data(df, most_recent_year_data=True)
    assert_frame_equal(result, exp_recent)