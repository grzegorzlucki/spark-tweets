from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

class TweetsLoader:
    def __init__(self):
        self.spark = SparkSession.builder.appName("TweetsLoader").getOrCreate()

    def load_tweets(self, csv_path, label):
        try:
            df = self.spark.read.option("header", "true").csv(csv_path)
            df = df.withColumn("category", lit(label))
            df = df.na.drop()
            return df
        except Exception as e:
            print(f"Error loading tweets from {csv_path}: {e}")
            return None

    def load_all_tweets(self, tweet_types):
        all_tweets = None
        for label, csv_path in tweet_types.items():
            tweets = self.load_tweets(csv_path, label)
            if tweets:
                if all_tweets:
                    all_tweets = all_tweets.unionByName(tweets, allowMissingColumns=True)
                else:
                    all_tweets = tweets
        return all_tweets