\# Spark Tweets Analysis Project

This project provides a set of tools for loading, cleaning, analyzing, and searching tweets using PySpark. The main components include:
- TweetsLoader: For loading tweets from CSV files.
- TweetsCleaner: For cleaning and preprocessing tweet data.
- TweetsAnalyzer: For analyzing various aspects of tweets.
- TweetsSearcher: For searching tweets based on specific criteria.

\## Table of Contents
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
  - [TweetsLoader](#tweetsloader)
  - [TweetsCleaner](#tweetscleaner)
  - [TweetsAnalyzer](#tweetsanalyzer)
  - [TweetsSearcher](#tweetssearcher)
  - [Example Notebook](#example-notebook)
- [Contributing](#contributing)
- [License](#license)

\## Project Structure

\```
spark-tweets/
├── analyzers/
│   └── tweets_analyzer.py
├── cleaners/
│   └── tweets_cleaner.py
├── loaders/
│   └── tweets_loader.py
├── searchers/
│   └── tweets_search.py
└── src/
    └── main.ipynb
\```

\## Installation

To use this project, you need to have Python and PySpark installed. Follow these steps to set up the environment:

1. **Install Python**: Ensure you have Python 3.7 or later installed.
2. **Install PySpark**: You can install PySpark using pip:
    \```bash
    pip install pyspark
    \```

\## Usage

\### TweetsLoader

The `TweetsLoader` class is responsible for loading tweet data from CSV files. It can load individual CSV files and merge them into a single DataFrame.

\```python
from tweets_loader import TweetsLoader

tweets_loader = TweetsLoader()
all_tweets_df = tweets_loader.load_all_tweets({
    "covid": "path/to/covid19_tweets.csv",
    "grammys": "path/to/GRAMMYs_tweets.csv",
    "financial": "path/to/financial.csv"
})
\```

\### TweetsCleaner

The `TweetsCleaner` class cleans and preprocesses the tweet data, such as removing unwanted characters from hashtags and converting columns to appropriate data types.

\```python
from tweets_cleaner import TweetsCleaner

tweets_cleaner = TweetsCleaner()
cleaned_tweets_df = tweets_cleaner.clean_all_tweets(all_tweets_df).cache()
\```

\### TweetsAnalyzer

The `TweetsAnalyzer` class provides various methods to analyze the tweets, including calculating the frequency of hashtags, retweets, and sources, as well as the average number of followers per location.

\```python
from tweets_analyzer import TweetsAnalyzer

tweets_analyzer = TweetsAnalyzer()
tweets_analyzer.calculate_hashtags(cleaned_tweets_df).show()
tweets_analyzer.calculate_is_retweet(cleaned_tweets_df).show()
tweets_analyzer.calculate_source(cleaned_tweets_df).show()
tweets_analyzer.calculate_avg_user_followers_per_location(cleaned_tweets_df).show()
\```

\### TweetsSearcher

The `TweetsSearcher` class allows for searching tweets based on specific keywords and filtering by user location.

\```python
from tweets_search import TweetsSearcher

tweets_searcher = TweetsSearcher()
tweets_searcher.search_by_key_words(["Trump"], cleaned_tweets_df).show()
tweets_searcher.only_in_location("United States", cleaned_tweets_df).show()
\```

\### Example Notebook

The `src/main.ipynb` Jupyter notebook demonstrates how to use the different components of this project. It includes examples of loading, cleaning, analyzing, and searching tweets.

\```python
import sys
sys.path.append('path/to/loaders')
sys.path.append('path/to/cleaners')
sys.path.append('path/to/analyzers')
sys.path.append('path/to/searchers')

from pyspark.sql import SparkSession
from tweets_loader import TweetsLoader
from tweets_cleaner import TweetsCleaner
from tweets_analyzer import TweetsAnalyzer
from tweets_search import TweetsSearcher

spark = SparkSession.builder \
        .appName("projekt") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()

tweets_loader = TweetsLoader()
tweets_cleaner = TweetsCleaner()
tweets_analyzer = TweetsAnalyzer()
tweets_searcher = TweetsSearcher()

all_tweets_df = tweets_loader.load_all_tweets({
    "covid": "path/to/covid19_tweets.csv",
    "grammys": "path/to/GRAMMYs_tweets.csv",
    "financial": "path/to/financial.csv"
})

cleaned_tweets_df = tweets_cleaner.clean_all_tweets(all_tweets_df).cache()

tweets_analyzer.calculate_hashtags(cleaned_tweets_df).show()
tweets_analyzer.calculate_is_retweet(cleaned_tweets_df).show()
tweets_analyzer.calculate_source(cleaned_tweets_df).show()
tweets_analyzer.calculate_avg_user_followers_per_location(cleaned_tweets_df).show()

tweets_searcher.search_by_key_words(["Trump"], cleaned_tweets_df).show()
tweets_searcher.only_in_location("United States", cleaned_tweets_df).show()
\```

\## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.