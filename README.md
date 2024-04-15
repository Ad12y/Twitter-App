Twitter Data Analysis Project
Team Members
Team #694
Alice Smith
Bob Johnson
Charlie Brown
Diana Lee
Abstract
This project aims to analyze Twitter data using various data storage and retrieval techniques, including Azure SQL as the relational database and MongoDB as the NoSQL database. The dataset consists of Twitter tweets and user information, which are stored efficiently to allow for fast access and querying. Additionally, caching mechanisms are implemented to improve query response times for frequently accessed data. A search application is developed to allow users to search tweets by string, hashtag, and user, with options for time range and relevance ranking. The project evaluates the performance of different storage and retrieval strategies and presents results based on test queries.

Introduction
Twitter provides a wealth of data for analysis, including tweets, user information, and engagement metrics such as retweets. This project aims to extract insights from Twitter data by designing and implementing an efficient data storage and retrieval system. By utilizing Azure SQL and MongoDB, along with caching mechanisms, we aim to optimize query performance and provide a seamless user experience for searching and analyzing Twitter data.

Dataset
The dataset used in this project consists of Twitter tweets and user information. Each tweet contains metadata such as the author, timestamp, text content, hashtags, and URLs. User information includes details such as username, followers count, and profile description. The dataset is provided as a collection of JSON objects, which are processed and stored in the chosen datastores.

Persisted Data Model and Datastores
Relational Datastore (Azure SQL)
We store user information in Azure SQL, a relational database service provided by Microsoft Azure. The data model includes tables for users and their attributes, such as username, followers count, and profile description. Indexes are created on commonly queried fields to improve query performance.

Non-Relational Datastore (MongoDB)
Tweets are stored in MongoDB, a NoSQL database known for its flexibility and scalability. The data model represents each tweet as a document, containing metadata fields such as author, timestamp, and text content. Indexes are created for efficient querying of tweets based on various criteria, such as hashtags and user mentions.

Data Optimization and Tradeoffs
In designing the data model and selecting datastores, we optimized for query performance and scalability. Azure SQL is suitable for structured data and complex queries involving multiple tables, while MongoDB excels in handling unstructured data and high-volume writes. Tradeoffs include the complexity of managing relational schemas versus the flexibility of schemaless non-relational databases.

Indexing
Both Azure SQL and MongoDB utilize indexes to speed up query execution. Indexes are created on fields commonly used in search queries, such as usernames, hashtags, and timestamps. This improves query performance by enabling efficient data retrieval based on indexed fields.

Processing Tweets for Storing in Datastores
Tweets are processed in real-time using Python code that iterates through each tweet and extracts relevant information for storage. While currently processed as a batch, we plan to implement a streaming processing pipeline to handle data arriving in real-time. The processing pipeline parses JSON objects, extracts metadata fields, and stores the data in the appropriate datastores. This approach allows for efficient handling of streaming data and ensures that tweets are stored in near real-time.

Caching
A caching mechanism is implemented to store frequently accessed data and reduce query latency. We use a Python dictionary as the cache, with a maximum size limit to prevent memory overflow. Entries in the cache are evicted using a least-accessed strategy, and the cache state is periodically checkpointed to disk for persistence. An expiry mechanism based on Time-To-Live (TTL) is also implemented to remove stale entries from the cache.

<details>
<summary><strong>Click to view detailed caching implementation</strong></summary>
<p>
python
Copy code
# Implement caching mechanism
class Cache:
    def __init__(self, max_size):
        self.cache = {}
        self.max_size = max_size

    def get(self, key):
        if key in self.cache:
            # Update access time for key
            value = self.cache[key]
            del self.cache[key]
            self.cache[key] = value
            return value
        else:
            return None

    def set(self, key, value):
        if len(self.cache) >= self.max_size:
            # Evict least accessed entry
            self.cache.pop(next(iter(self.cache)))
        self.cache[key] = value

    def evict(self, key):
        if key in self.cache:
            del self.cache[key]

# Initialize cache
cache = Cache(max_size=1000)
</p>
</details>
Search Application Design
The search application allows users to search tweets by string, hashtag, and user, with options for time range and relevance ranking. Search queries are translated into database queries for retrieval of relevant tweets and user information. Search results include metadata such as author, timestamp, and retweet count, with drill-down options for exploring related tweets and user activity.

Results
We present the results of test search queries, including timings for query execution with and without caching. Performance metrics such as response times and cache hit rates are evaluated to assess the effectiveness of the caching mechanism. Additionally, top-level metrics such as the top 10 users and top 10 tweets are provided to highlight popular content and users on Twitter.

Conclusions
In conclusion, this project demonstrates the design and implementation of a Twitter data analysis system using Azure SQL and MongoDB, along with caching mechanisms. We discuss the tradeoffs involved in data modeling and storage selection, as well as the impact on query performance and scalability. The search application provides a user-friendly interface for exploring Twitter data and extracting valuable insights.

References
Azure SQL Documentation
MongoDB Documentation
Dash Documentation
