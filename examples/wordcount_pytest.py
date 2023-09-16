from typing import List

import pytest
from pyspark.sql.types import StringType, Row

from . import word_count


@pytest.mark.usefixtures('spark_session')
def test_word_count(spark_session):
    df = spark_session.createDataFrame(
        ['Hello Spark', 'Hello again, Spark', 'Apache Spark', 'PySpark is Python and Spark'],
        StringType()
    )
    counts = word_count.count_words(df).head(2)

    spark_count = counts[0]
    assert spark_count['word'] == 'Spark'
    assert spark_count['count'] == 4

    hello_count = counts[1]
    assert hello_count['word'] == 'Hello'
    assert hello_count['count'] == 2
