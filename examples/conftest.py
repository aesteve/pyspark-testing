""" The file must be named conftest.py by convention, and contains Fixtures that will then be injected in tests
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(request: pytest.FixtureRequest) -> SparkSession:
    session = SparkSession.builder \
        .appName("pyspark-test-app") \
        .master("local[*]") \
        .getOrCreate()
    request.addfinalizer(lambda: session.stop())
    return session
