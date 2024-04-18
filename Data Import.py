# Databricks notebook source
# MAGIC %sh 
# MAGIC pip install pymongo

# COMMAND ----------

import json
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
import os

class DataProcessor:
    def __init__(self, spark, file_path):
        self.file_path = file_path
        self.spark = spark
        self.list_of_user_dic = []
        self.list_of_tweets_dic = []
    
    def parse_datetime(self, timestamp_str):
        return datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S %z %Y')
    
    def parse_user(self, user_data):
        user = {
            "user_id": user_data["id"],
            "name": user_data["name"],
            "screen_name": user_data["screen_name"],
            "location": user_data.get("location", ""),
            "description": user_data.get("description", ""),
            "verified": user_data["verified"],
            "profile_picture": user_data["profile_image_url_https"],
            "followers_count": user_data["followers_count"],
            "friends_count": user_data["friends_count"],
            "listed_count": user_data["listed_count"],
            "favourites_count": user_data["favourites_count"],
            "statuses_count": user_data["statuses_count"],
            "created_at": self.parse_datetime(user_data["created_at"]),
            "geo_enabled": user_data["geo_enabled"]
        }
        return user
    
    def parse_tweet(self, tweet_data):
        tweet = {
            "tweet_id": tweet_data["id"],
            "user_id": tweet_data["user"]["id"],
            "created_at": self.parse_datetime(tweet_data["created_at"]),
            "text": tweet_data["text"],
            "source": tweet_data["source"],
            "quote_count": tweet_data["quote_count"],
            "reply_count": tweet_data["reply_count"],
            "retweet_count": tweet_data["retweet_count"],
            "favorite_count": tweet_data["favorite_count"],
            "filter_level": tweet_data["filter_level"],
            "lang": tweet_data["lang"], 
        }
        return tweet
    
    def replace_null_false_true(self, obj):
        if isinstance(obj, dict):
            return {k: self.replace_null_false_true(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.replace_null_false_true(elem) for elem in obj]
        elif obj == "null":
            return None
        elif obj == "false":
            return False
        elif obj == "true":
            return True
        else:
            return obj
    
    def process_data(self):
        with open(self.file_path, 'r') as file:
            for line in file:
                if line != '\n':
                    python_dict = json.loads(self.replace_null_false_true(line))
                    user_obj = self.parse_user(python_dict["user"])
                    tweet_obj = self.parse_tweet(python_dict)
                    try:
                        user_obj_rt = self.parse_user(python_dict['retweeted_status']["user"])
                        tweet_obj_rt = self.parse_tweet(python_dict['retweeted_status'])
                        tweet_obj["retweet_id"] = tweet_obj_rt['tweet_id']
                        tweet_obj_rt["retweet_id"] = None
                        self.list_of_user_dic.append(user_obj_rt)
                        self.list_of_tweets_dic.append(tweet_obj_rt)
                    except:
                        tweet_obj["retweet_id"] = None
                        pass
                    self.write_to_sql_server(tweet_obj)
                    self.write_to_mongodb(user_obj)
    
    def write_to_sql_server(self, tweet_obj):

        # Define the schema for the tweet data
        schema = StructType([
            StructField("tweet_id", LongType(), True),
            StructField("user_id", LongType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("text", StringType(), True),
            StructField("source", StringType(), True),
            StructField("quote_count", LongType(), True),
            StructField("reply_count", LongType(), True),
            StructField("retweet_count", LongType(), True),
            StructField("favorite_count", LongType(), True),
            StructField("filter_level", StringType(), True),
            StructField("lang", StringType(), True),
            StructField("retweet_id", LongType(), True)
        ])

        df_tweet = self.spark.createDataFrame([tweet_obj], schema = schema)
        connection_properties = {
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        df_tweet.write.jdbc(url=os.environ.get("JDBC_URL"), table="tweet", mode="append", properties=connection_properties)
    
    def write_to_mongodb(self, user_obj):
        client = MongoClient(os.environ.get("MONGO_URI"), server_api=ServerApi('1'))
        db = client['Twitter']
        collection = db['Users']
        collection.update_one({"user_id": user_obj["user_id"]}, {"$set": user_obj}, upsert=True)
# Usage
file_path = '/dbfs/FileStore/shared_uploads/as4622@scarletmail.rutgers.edu/corona_out_3'
data_processor = DataProcessor(spark, file_path)
data_processor.process_data()
