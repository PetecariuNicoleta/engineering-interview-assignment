import sys
import os
import pytest
from pyspark.sql import SparkSession
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from solution.utils import clean_data

@pytest.fixture
def spark():
    return SparkSession.builder\
        .master("local[1]")\
        .appName("test")\
        .getOrCreate()

def test_remove_duplicates_races(spark):
    data = [
        (1, 2024, "British GP", "2024-07-07"),
        (1, 2024, "British GP", "2024-07-07")  # duplicate
    ]

    columns = ["raceid", "year", "name", "date"]
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_data(df, "races")
    assert cleaned_df.count()==1

def test_remove_null_races(spark):
    data = [
        (1, 2024, "British GP", "2024-07-07"),
        (1, None, "British GP", "2024-07-07")  # null year
    ]

    columns = ["raceid", "year", "name", "date"]
    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_data(df, "races")
    assert cleaned_df.count()==1

def test_position_cast_results(spark):
    data = [
        (1, 10, 44, "1"),
        (2, 10, 16, "null")
    ]

    columns = ["resultid", "raceid", "driverid", "position"]

    df = spark.createDataFrame(data, columns)
    cleaned_df = clean_data(df, "results")
    assert cleaned_df.count()==1

