# Take care of any imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# Create the Spark Context

spark = SparkSession.builder.config("spark.driver.host", "192.168.0.3") \
    .config("spark.ui.port", "4060").appName("new-connect").getOrCreate()

# Complete the script


path = "../../data/sparkify_log_small.json"

spark.sparkContext.setLogLevel("ERROR")

user_info_df=spark.read.json(path)

# # Data Exploration 
user_info_df.show(5)
# # Explore the data set.


# View 5 records
# print(user_info_df.head(5))

# Print the schema
user_info_df.printSchema()

# Describe the dataframe
user_info_df.describe().show()

# Describe the statistics for the song length column
# user_info_df.describe("song").show()

# Count the rows in the dataframe
# print(user_info_df.count())

# Select the page column, drop the duplicates, and sort by page
# user_info_df.select('page').dropDuplicates().sort('page').show()

# Select data for all pages where userId is 1046
# user_info_df.select(["userId", "firstname", "page", "song"]).where(user_info_df.userId == 1046).show()

# # Calculate Statistics by Hours
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).hour)

user_info_df = user_info_df.withColumn("hour", get_hour(user_info_df.ts),)

# user_info_df.show(1)
# Select just the NextSong page
user_info_df.where(user_info_df.page == "NextSong").groupBy("hour").count().orderBy(user_info_df.hour.cast("float")).show()

# # Drop Rows with Missing Values

valid_user_info = user_info_df.dropna( how="any", subset=["userId","sessionId"])
# How many are there now that we dropped rows with null userId or sessionId?

valid_user_info.select(["userId","gender"]).dropDuplicates().groupBy("gender").count().show()

# select all unique user ids into a dataframe

valid_user_info.select('userId').dropDuplicates().show()
print(valid_user_info.count())

# Select only data for where the userId column isn't an empty string (different from null)
valid_user_info = valid_user_info.where(valid_user_info.userId != "")

print(valid_user_info.count())

# # Users Downgrade Their Accounts
valid_user_info.filter("page = 'Submit Downgrade'").show()

# Find when users downgrade their accounts and then show those log entries. 
valid_user_info.select(['userId', 'firstName','page','level','song']).where(valid_user_info.userId ==1138).show()

# Create a user defined function to return a 1 if the record contains a downgrade
mark_downgrade = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())
# Select data including the user defined function

valid_user_info = valid_user_info.withColumn('downgraded',mark_downgrade(valid_user_info.page))

# Partition by user id


from pyspark.sql import Window

windowval = Window.partitionBy("userId") \
    .orderBy(desc("ts")) \
    .rangeBetween(Window.unboundedPreceding, 0)
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.

# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have
# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
valid_user_info = valid_user_info.withColumn("phase", Fsum("downgraded").over(windowval))



# Show the phases for user 1138 

valid_user_info \
    .select(["userId", "firstname", "ts", "page", "level", "phase"]) \
    .where(valid_user_info.userId == "1138") \
    .sort("ts") \
    .show()