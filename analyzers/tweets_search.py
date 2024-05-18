from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, array_intersect, size, lit

class TweetsSearcher:
    text_column = "text"
    user_location_column = "user_location"
    
    def __init__(self):
        self.spark = SparkSession.builder.appName("TweetsSearch").getOrCreate()
        
    def search_by_column(self, key_word, df):
        data_frame = df.filter(col(self.text_column).contains(key_word))
        return data_frame
    
    def search_by_key_words(self, key_words, df):
        data_frame = df.withColumn("words", split(col(self.text_column), " ")) \
            .withColumn("keyWordsResult", array_intersect(col("words"), lit(key_words))) \
            .filter(size(col("keyWordsResult")) > 0) \
            .drop("words", "keyWordsResult")
        return data_frame
    
    def only_in_location(self, location, df):
        data_frame = df.filter(col(self.user_location_column).equalTo(location))
        return data_frame
        
    def only_in_location(self, location, df):
        data_frame = df.filter(col(self.user_location_column).equalTo(location))
        return data_frame