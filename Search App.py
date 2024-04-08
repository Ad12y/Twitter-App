# Databricks notebook source
# Start Time of Time Period
dbutils.widgets.text("time_period_start", "2010-03-22 11:49:18", "Start Time of Time Period")

# End Time of Time Period
dbutils.widgets.text("time_period_end", "2020-04-25 14:48:35", "End Time of Time Period")

# Keyword/Hashtag Search
dbutils.widgets.text("keyword", "Modi", "Keyword/Hashtag Search")

# Username Search
dbutils.widgets.text("username", "", "Username Search")

# Sorting Parameter
dbutils.widgets.dropdown("param_name", "retweet_count", ["retweet_count","followers_count", "favourites_count", "friends_count", "reply_count"], "Sorting Parameter")

# Choose Order
dbutils.widgets.dropdown("order", "Descending", ["Descending", "Ascending"], "Choose Order")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, collect_list
from datetime import datetime
import pandas as pd

class TwitterDataProcessor:
    def __init__(self, spark, mongo_uri, jdbc_url):
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark = spark
        self.mongo_uri = mongo_uri
        self.jdbc_url = jdbc_url
        self.keyword = None
        self.time_period_start = None
        self.time_period_end = None
        self.df_users = None
        self.param_name = None
        self.order = None
        self.username = None
        self.df_tweets = None
        self.df_user_tweets = None
    
    def load_users_data(self):
        df_users = (
            self.spark.read.format("mongodb")
            .option("database", "Twitter")
            .option("spark.mongodb.read.connection.uri", self.mongo_uri)
            .option("collection", "Users")
            .load()
        )
        # Cache the DataFrame
        df_users.cache()
        self.df_users =  df_users.withColumnRenamed("user_id", "user_id_1") \
                    .withColumnRenamed("created_at", "user_created_at")
        
    def load_tweets_data(self):
        df_tweets = self.spark.read.jdbc(url=self.jdbc_url, table="tweet", properties=self.get_connection_properties())
        # Cache the DataFrame
        df_tweets.cache()
        self.df_tweets = df_tweets
    
    def get_connection_properties(self):
        return {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    
    def filter_tweets_by_keyword(self, df_tweets):
        return df_tweets.filter(col("text").like("%{}%".format(self.keyword)))
    
    def filter_tweets_by_username(self, df_users):
        return df_users.filter(col("name").like("%{}%".format(self.username)))
    
    def filter_tweets_by_time_period(self, df_tweets):
        df_tweets = df_tweets.withColumn("created_at", to_timestamp("created_at", "EEE MMM dd HH:mm:ss Z yyyy"))
        return df_tweets.filter((col("created_at") >= self.time_period_start) & (col("created_at") <= self.time_period_end))
    
    def sort_user_tweets(self, filtered_twitter_df):
        ascending = True if self.order == "Ascending" else False
        return filtered_twitter_df.sort(self.param_name, ascending=ascending)

    def apply_filters(self):
        filtered_df_tweets = self.filter_tweets_by_keyword(self.df_tweets)
        filtered_df_tweets = self.filter_tweets_by_time_period(filtered_df_tweets)
        filtered_df_users = self.filter_tweets_by_username(self.df_users)
        filtered_twitter_df = filtered_df_tweets.join(filtered_df_users, filtered_df_tweets["user_id"] == filtered_df_users["user_id_1"], how="left")
        twitter_df = self.df_tweets.join(self.df_users, self.df_tweets["user_id"] == self.df_users["user_id_1"], how="left")
        twitter_df = twitter_df['screen_name','name','tweet_id', 'retweet_id','created_at','text']
        aggregated_df = twitter_df.groupBy("retweet_id").agg(
            collect_list("name").alias("names"),
            collect_list("text").alias("texts"),
            collect_list("created_at").alias("created_ats")
        )
        retweeted_df = filtered_twitter_df.join(aggregated_df, filtered_twitter_df["tweet_id"] == aggregated_df["retweet_id"], how="left")
        sorted_twitter_df  = self.sort_user_tweets(retweeted_df)
        return sorted_twitter_df
    
    # def generate_tweet_html(self, tweet):
        retweet_times = tweet['created_ats']
        retweet_texts = tweet['texts']
        retweet_users = tweet['names']
        user_name = tweet['name']
        user_screen_name = tweet['screen_name']
        user_profile_image_url = tweet['profile_picture']
        user_description = tweet['description']
        user_verified = tweet['verified']
        user_followers = tweet['followers_count']
        user_friends = tweet['friends_count']
        user_favourites =  tweet['favourites_count']
        user_location = tweet.get('location', '')
        tweet_text = tweet['text']
        retweet_count = tweet['retweet_count']
        favorite_count = tweet['favorite_count']
        source = tweet['source']
        created_at = pd.to_datetime(tweet['created_at'])
        
        user_verified_html = '<span style="color: blue; font-weight: bold;">&#x2713;</span>' if user_verified else ''
        
        html_content = f"""
        <div style="border: 1px solid #ccc; border-radius: 15px; padding: 15px; margin: 20px 0; background-color: #f5f8fa;">
            <div style="display: flex; align-items: flex-start;">
                <a href="#" onclick="toggleAdditionalInfo();fetchRecentPosts(); return false;">
                    <img src="{user_profile_image_url}" style="width: 50px; height: 50px; border-radius: 50%; margin-right: 15px;">
                </a>
                <div>
                    <div style="display: flex; align-items: center;">
                        <span style="font-weight: bold; font-size: 16px;">{user_name}</span>
                        {user_verified_html}
                    </div>
                    <p style="margin: 5px 0; font-size: 14px; color: #657786;">@{user_screen_name}</p>
                    <p style="margin: 5px 0; font-size: 14px; color: #657786;">{user_description}</p>
                </div>
            </div>
            <p style="margin: 15px 0; font-size: 18px; line-height: 1.4;">{tweet_text}</p>
            <div style="display: flex; justify-content: space-between; align-items: center; color: #657786; font-size: 14px;">
                <div style="display: flex;">
                    <a href="#" onclick="fetchRecentRetweets(); return false;" style="text-decoration: none; color: #1da1f2;">
                        <strong>{retweet_count}</strong> Retweets
                    </a>
                    <img src="https://abs.twimg.com/icons/apple-touch-icon-192x192.png" style="width: 20px; height: 20px; margin: 0 5px;">
                    <span>{source}</span>
                </div>
                <div style="display: flex;">
                    <span style="margin-right: 20px;">{created_at}</span>
                    <span><strong>{favorite_count}</strong> Likes</span>
                </div>
            </div>
            <div id="additionalInfo" style="display: none;">
                <h4 style="font-size: 16px; margin-top: 17px;">Profile Information:</h4>
                <p><strong>Location:</strong> {user_location}</p>
                <p><strong>Followers:</strong> {user_followers}</p>
                <p><strong>Friends:</strong> {user_friends}</p>
                <p><strong>Favourites:</strong> {user_favourites}</p>
            </div>
            <div id="recentRetweets" style="display: none;">
                <h4 style="font-size: 16px; margin-top: 15px;">Recent Retweets:</h4>
                <ul id="recentRetweetsList" style="list-style-type: none; padding-left: 0;">
                    {"".join(f'<li style="margin-bottom: 10px;">'
                            f'<div style="background-color: #fff; border: 1px solid #ccc; border-radius: 5px; padding: 10px;">'
                            f'<span style="color: #1da1f2; font-weight: bold;">{retweet_users[i]}</span>'
                            f'<span> retweeted at {retweet_times[i]}: {retweet_texts[i]}</span>'
                            f'</div>'
                            f'</li>' for i in range(len(retweet_times)))}
                </ul>
            </div>
            <div id="recentPosts" style="display: none;">
                <h4 style="font-size: 16px; margin-top: 17px;">Recent Posts:</h4>
                <ul id="recentPostsList" style="list-style-type: none; padding-left: 0;">
                    <li style="margin-bottom: 10px;">
                        <div style="background-color: #fff; border: 1px solid #ccc; border-radius: 5px; padding: 10px;">
                            <span style="color: #1da1f2; font-weight: bold;">2024-04-02 10:00:00:</span>
                            <span>Namaste india 🙏 hum sab corona virus ko harane mein ek saath hai , hum sab apne apne sarkar ki baat ka nirdes kare…</span>
                        </div>
                    </li>
                    <li style="margin-bottom: 10px;">
                        <div style="background-color: #fff; border: 1px solid #ccc; border-radius: 5px; padding: 10px;">
                            <span style="color: #1da1f2; font-weight: bold;">2024-04-02 11:00:00:</span>
                            <span>The Govt announcement today of a financial assistance package, is the first step in the right direction. India owes…</span>
                        </div>
                    </li>
                    <li style="margin-bottom: 10px;">
                        <div style="background-color: #fff; border: 1px solid #ccc; border-radius: 5px; padding: 10px;">
                            <span style="color: #1da1f2; font-weight: bold;">2024-04-02 12:00:00:</span>
                            <span>The future of the nation is ensuring the nation’s future. Excellent gesture, Sahil, proud of you. #IndiaFightsCorona</span>
                        </div>
                    </li>
                </ul>
            </div>
        </div>
        <script>
            function toggleAdditionalInfo() {{
                var additionalInfo = document.getElementById('additionalInfo');
                if (additionalInfo.style.display === 'none') {{
                    additionalInfo.style.display = 'block';
                }} else {{
                    additionalInfo.style.display = 'none';
                }}
            }}

            function fetchRecentRetweets() {{
                var recentRetweetsDiv = document.getElementById('recentRetweets');
                if (recentRetweetsDiv.style.display === 'none') {{
                    recentRetweetsDiv.style.display = 'block';
                }} else {{
                    recentRetweetsDiv.style.display = 'none';
                }}
            }}

            function fetchRecentPosts() {{
                var recentPostsDiv = document.getElementById('recentPosts');
                if (recentPostsDiv.style.display === 'none') {{
                    recentPostsDiv.style.display = 'block';
                }} else {{
                    recentPostsDiv.style.display = 'none';
                }}
            }}
        </script>
        """

        return html_content
    def generate_tweet_html(self, tweet):
        retweet_times = tweet['created_ats']
        retweet_texts = tweet['texts']
        retweet_users = tweet['names']
        user_name = tweet['name']
        user_screen_name = tweet['screen_name']
        user_profile_image_url = tweet['profile_picture']
        user_description = tweet['description']
        user_verified = tweet['verified']
        user_followers = tweet['followers_count']
        user_friends = tweet['friends_count']
        user_favourites =  tweet['favourites_count']
        user_location = tweet.get('location', '')
        tweet_text = tweet['text']
        retweet_count = tweet['retweet_count']
        favorite_count = tweet['favorite_count']
        source = tweet['source']
        created_at = pd.to_datetime(tweet['created_at'])
        
        user_verified_html = '<span style="color: blue; font-weight: bold;">&#x2713;</span>' if user_verified else ''
        
        # Check if tweet['texts'] is None
        if retweet_texts is None:
            disable_retweet_button = "pointer-events: none; color: #ccc;"
        else:
            disable_retweet_button = ""

        # Limit the number of retweets displayed to a maximum of 5
        retweet_list_html = ""
        if retweet_texts:
            for i in range(min(len(retweet_times), 5)):
                retweet_list_html += f'<li style="margin-bottom: 10px;">' \
                                    f'<div style="background-color: #fff; border: 1px solid #ccc; border-radius: 5px; padding: 10px;">' \
                                    f'<span style="color: #1da1f2; font-weight: bold;">{retweet_users[i]}</span>' \
                                    f'<span> retweeted at {retweet_times[i]}: {retweet_texts[i]}</span>' \
                                    f'</div>' \
                                    f'</li>'

        html_content = f"""
        <div style="border: 1px solid #ccc; border-radius: 15px; padding: 15px; margin: 20px 0; background-color: #f5f8fa;">
            <div style="display: flex; align-items: flex-start;">
                <a href="#" onclick="toggleAdditionalInfo();fetchRecentPosts(); return false;">
                    <img src="{user_profile_image_url}" style="width: 50px; height: 50px; border-radius: 50%; margin-right: 15px;">
                </a>
                <div>
                    <div style="display: flex; align-items: center;">
                        <span style="font-weight: bold; font-size: 16px;">{user_name}</span>
                        {user_verified_html}
                    </div>
                    <p style="margin: 5px 0; font-size: 14px; color: #657786;">@{user_screen_name}</p>
                    <p style="margin: 5px 0; font-size: 14px; color: #657786;">{user_description}</p>
                </div>
            </div>
            <p style="margin: 15px 0; font-size: 18px; line-height: 1.4;">{tweet_text}</p>
            <div style="display: flex; justify-content: space-between; align-items: center; color: #657786; font-size: 14px;">
                <div style="display: flex;">
                    <a href="#" onclick="fetchRecentRetweets(); return false;" style="text-decoration: none; color: #1da1f2; {disable_retweet_button}">
                        <strong>{retweet_count}</strong> Retweets
                    </a>
                    <img src="https://abs.twimg.com/icons/apple-touch-icon-192x192.png" style="width: 20px; height: 20px; margin: 0 5px;">
                    <span>{source}</span>
                </div>
                <div style="display: flex;">
                    <span style="margin-right: 20px;">{created_at}</span>
                    <span><strong>{favorite_count}</strong> Likes</span>
                </div>
            </div>
            <div id="additionalInfo" style="display: none;">
                <h4 style="font-size: 16px; margin-top: 17px;">Profile Information:</h4>
                <p><strong>Location:</strong> {user_location}</p>
                <p><strong>Followers:</strong> {user_followers}</p>
                <p><strong>Friends:</strong> {user_friends}</p>
                <p><strong>Favourites:</strong> {user_favourites}</p>
            </div>
            <div id="recentRetweets" style="display: none;">
                <h4 style="font-size: 16px; margin-top: 15px;">Recent Retweets:</h4>
                <ul id="recentRetweetsList" style="list-style-type: none; padding-left: 0;">
                    {retweet_list_html}
                </ul>
            </div>
            <div id="recentPosts" style="display: none;">
                <h4 style="font-size: 16px; margin-top: 17px;">Recent Posts:</h4>
                <ul id="recentPostsList" style="list-style-type: none; padding-left: 0;">
                    <!-- Your recent posts HTML here -->
                </ul>
            </div>
        </div>
        <script>
            function toggleAdditionalInfo() {{
                var additionalInfo = document.getElementById('additionalInfo');
                if (additionalInfo.style.display === 'none') {{
                    additionalInfo.style.display = 'block';
                }} else {{
                    additionalInfo.style.display = 'none';
                }}
            }}

            function fetchRecentRetweets() {{
                var recentRetweetsDiv = document.getElementById('recentRetweets');
                if (recentRetweetsDiv.style.display === 'none') {{
                    recentRetweetsDiv.style.display = 'block';
                }} else {{
                    recentRetweetsDiv.style.display = 'none';
                }}
            }}

            function fetchRecentPosts() {{
                var recentPostsDiv = document.getElementById('recentPosts');
                if (recentPostsDiv.style.display === 'none') {{
                    recentPostsDiv.style.display = 'block';
                }} else {{
                    recentPostsDiv.style.display = 'none';
                }}
            }}
        </script>
        """

        return html_content

# Define MongoDB URI and JDBC URL
mongo_uri = "mongodb+srv://as4622:Gateway!123@cluster0.nbxrocy.mongodb.net/?retryWrites=true&w=majority"
jdbc_url = "jdbc:sqlserver://twitterdb.database.windows.net:1433;database=Twitter_db;user=CloudSAbf912dc9@twitterdb;password=Gateway!123;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=3600"

# Create TwitterDataProcessor instance
twitter_processor = TwitterDataProcessor(spark, mongo_uri, jdbc_url)

# Apply filter and display results
twitter_processor.load_users_data()
twitter_processor.load_tweets_data()
cache_dic = {}


# COMMAND ----------

# Set keyword and time period
twitter_processor.keyword = dbutils.widgets.get("keyword")
twitter_processor.time_period_start = datetime.strptime(dbutils.widgets.get("time_period_start"), "%Y-%m-%d %H:%M:%S")
twitter_processor.time_period_end = datetime.strptime(dbutils.widgets.get("time_period_end"), "%Y-%m-%d %H:%M:%S")
twitter_processor.order = dbutils.widgets.get("order")
twitter_processor.param_name = dbutils.widgets.get("param_name")
twitter_processor.username =  dbutils.widgets.get("username")
parameter_name = f"{dbutils.widgets.get('keyword').lower()}_{dbutils.widgets.get('time_period_start')}_{dbutils.widgets.get('time_period_end')}_{dbutils.widgets.get('order')}_{dbutils.widgets.get('param_name')}_{dbutils.widgets.get('username')}"

if parameter_name in cache_dic:
    pass
else:
    df = twitter_processor.apply_filters()
    # Selecting and renaming columns
    selected_columns = df.select(
        "created_at",
        "favorite_count",
        "retweet_count",
        "source",
        "text",
        "user_id",
        "name",
        "screen_name",
        "profile_picture",
        "description",
        "verified",
        "followers_count",
        "friends_count",
        "favourites_count",
        "location",
        "created_ats",
        "texts",
        "names"
    )

    # Limit the data early
    selected_columns = selected_columns.limit(100)

    # Collect top 5 records as list of dictionaries
    top_records = selected_columns.collect()

    tweets_lst = []
    for record in top_records:
        tweets_lst.append(record.asDict())
    cache_dic[parameter_name] = tweets_lst

for tweet in cache_dic[parameter_name][0:15]:
    displayHTML(twitter_processor.generate_tweet_html(tweet))
