# # Data Wrangling with DataFrames Coding Quiz

from pyspark.sql import SparkSession

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import desc,asc

spark = SparkSession.builder.appName("quiz").getOrCreate()
path = '../../data/sparkify_log_small.json'

sparkify_df = spark.read.json(path)

sparkify_df.show(5)
# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# TODO: write your code to answer question 1

empty_pages = sparkify_df.select(['page']).where(sparkify_df.userId == '').dropDuplicates()

all_pages = sparkify_df.select('page').dropDuplicates()

did_not_visit = all_pages.subtract(empty_pages)

# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?



# TODO: use this space to explore the behavior of the user with an empty string
user_type = 'unregistered'

# # Question 3
# 
# How many female users do we have in the data set?

# TODO: write your code to answer question 3

# sparkify_df.select(['gender', 'userId']).dropDuplicates(['userId']).groupBy('gender').count().show()
# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4
# sparkify_df.select(['artist']).groupBy(['artist']).agg(F.count('artist').alias("artist_count")).orderBy('artist_count', ascending=False).show()

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 


# TODO: write your code to answer question 5

songs_per_user = sparkify_df.select('userId').where(sparkify_df.page != "Home").groupBy('userId').count()

songs_per_user.agg(F.round(F.avg('count')).alias("user_count")).show()