from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    # Creating a Spark Session
    spark = SparkSession.builder \
        .appName("PySpark Example App") \
        .getOrCreate()

    # Creating unity_golf_club dataframe
    unity_golf_club_df = spark.read.csv("unity_golf_club.csv", header=True, inferSchema=True)

    # Creating us_softball_league dataframe
    us_softball_league_df = spark.read.csv("us_softball_league.tsv", sep = '\t', header=True, inferSchema=True)

    # Stop the SparkSession
    spark.stop()
