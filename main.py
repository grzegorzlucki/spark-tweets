import sys
from IPython.core.display import HTML, display
import os

sys.path.append('./loaders')
sys.path.append('./cleaners')
sys.path.append('./analyzers')
display(HTML("<style>pre { white-space: pre !important; }</style>"))

os.environ['PYSPARK_HOME'] = "C:/Users/USER/Desktop/spark-kurs/Projekt"
os.environ['PYSPARK_DRIVER_PYTHON'] = "jupyter"
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

try:
    from pyspark.sql import SparkSession, Row
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType
    from pyspark.sql.functions import col, split, explode, sha2, round, avg, udf
except ImportError as e:
    print(f"Error importing PySpark modules: {e}")

try:
    from tweets_loader import TweetsLoader
    from tweets_cleaner import TweetsCleaner
    from tweets_analyzer import TweetsAnalyzer
    from tweets_search import TweetsSearcher
except ImportError as e:
    print(f"Error importing project modules: {e}")
    
try:
    spark = SparkSession.builder \
            .appName("projekt") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .master("local[*]") \
            .getOrCreate()
except Exception as e:
    print(f"Error creating Spark session: {e}")
    
tweets_loader = TweetsLoader()
tweets_cleaner = TweetsCleaner()
tweets_analyzer = TweetsAnalyzer()
tweets_searcher = TweetsSearcher()

all_tweets_df = tweets_loader.load_all_tweets({
    "covid": "./data/covid19_tweets.csv",
    "grammys": "./data/GRAMMYs_tweets.csv",
    "financial": "./data/financial.csv"
})

tweets_cleaner = tweets_cleaner.clean_all_tweets(all_tweets_df).cache()

tweets_analyzer.calculate_hashtags(tweets_cleaner).show()
tweets_analyzer.calculate_is_retweet(tweets_cleaner).show()
tweets_analyzer.calculate_source(tweets_cleaner).show()
tweets_analyzer.calculate_avg_user_followers_per_location(tweets_cleaner).show()

# tweets_searcher.search_by_key_words(["Trump"], tweets_cleaner).transform(onlyInLocation("United States", )).show()
spark.stop()