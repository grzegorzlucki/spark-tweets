from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, split
from pyspark.sql.types import LongType, DateType, DataType

class TweetsCleaner:
    def __init__(self):
        self.spark = SparkSession.builder.appName("TweetsCleaner").getOrCreate()

    def clean_all_tweets(self, df):
        cleaned_df = df \
            .withColumn("hashtags", regexp_replace(col("hashtags"), "[']", "")) \
            .withColumn("hashtags", regexp_replace(col("hashtags"), "\\[", "")) \
            .withColumn("hashtags", regexp_replace(col("hashtags"), "\\]", "")) \
            .withColumn("hashtags", split(col("hashtags"), ",")) \
            .withColumn("date", col("date").cast(DateType())) \
            .withColumn("user_created", col("user_created").cast(DateType())) \
            .withColumn("user_favourites", col("user_favourites").cast(LongType())) \
            .withColumn("user_friends", col("user_friends").cast(LongType())) \
            .withColumn("user_followers", col("user_followers").cast(LongType()))
        
        return cleaned_df