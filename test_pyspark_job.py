import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("PySpark-CI-Test")
        .getOrCreate()
    )


def test_clean_data(spark):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])

    data = [
        ("Mai", 100.0),
        ("Adam", 200.0),
        ("Aya", 0.0),
        ("Reham", -50.0),
        ("Eslam", 300.0),
        (None, 400.0)
    ]

    df = spark.createDataFrame(data, schema)

    result = clean_data(df)

    rows = result.collect()

    # Check that only valid records remain
    assert len(rows) == 3

    # Check that amount <= 0 was removed
    amounts = [row["amount"] for row in rows]
    assert all(amount > 0 for amount in amounts)

    # Check that NULL names were removed
    names = [row["name"] for row in rows]
    assert None not in names

    # Check amount_with_tax
    tax_values = {
        row["name"]: row["amount_with_tax"]
        for row in rows
    }

    assert tax_values["Mai"] == pytest.approx(120.0)
    assert tax_values["Adam"] == pytest.approx(240.0)
    assert tax_values["Eslam"] == pytest.approx(360.0)