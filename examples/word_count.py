from pyspark.sql import DataFrame
import pyspark.sql.functions as f


def count_words(lines: DataFrame) -> DataFrame:
    return lines.withColumn('word', f.explode(f.split(f.col('value'), ' '))) \
                    .groupby('word') \
                    .count() \
                    .sort('count', ascending=False)

