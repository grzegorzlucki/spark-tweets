from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, split, explode_outer, avg

class TweetsAnalyzer:
    hashtag_column = "hashtags"
    is_retweet_column = "is_retweet"
    source_column = "source"
    user_followers_column = "user_followers"
    user_name_column = "user_name"
    user_location_column = "user_location"
    
    def __init__(self):
        self.spark = SparkSession.builder.appName("TweetsAnalyzer").getOrCreate()
        
    def calculate_hashtags(self, df):
        data_frame = df.withColumn(self.hashtag_column, explode_outer(col(self.hashtag_column))) \
                       .groupBy(self.hashtag_column).count()
        return data_frame
        
    def calculate_is_retweet(self, df):
        data_frame = df.groupBy(self.is_retweet_column).count()
        return data_frame

    def calculate_source(self, df):
        data_frame = df.groupBy(self.source_column).count()
        return data_frame    

    def calculate_avg_user_followers_per_location(self, df):
        data_frame = df.select(self.user_name_column, self.user_followers_column, self.user_location_column) \
                       .filter(col(self.user_name_column).isNotNull()) \
                       .filter(col(self.user_location_column).isNotNull()) \
                       .dropDuplicates([self.user_name_column]) \
                       .groupBy(self.user_location_column) \
                       .agg(avg(self.user_followers_column).alias("avg_user_followers"))
        return data_frame