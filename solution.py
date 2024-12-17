from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import json

# Standardizing us_softball_league dataframe
def standardize_df(df, state_map):
    nameArr = split(df.name, " ")
    df = (df
            # Split full names into first_name and last_name
            .withColumn("first_name", nameArr.getItem(0)) 
            .withColumn("last_name", nameArr.getItem(1))
            .drop("name")

            # Change format of dates
            .withColumn("date_of_birth", to_date(col("date_of_birth"), "MM/dd/yyyy"))
            .withColumn("dob", date_format(col("date_of_birth"), "yyyy/MM/dd"))
            .drop("date_of_birth")
            .withColumn("last_active", to_date(col("last_active"), "MM/dd/yyyy"))
            .withColumn("last_active", date_format(col("last_active"), "yyyy/MM/dd"))

            # Replace state names with abbreviation
            .withColumn("state", state_map[col("us_state")])
            .drop("us_state")

            .withColumnRenamed("joined_league", "member_since")
            .select("first_name", "last_name", "dob", "company_id", "last_active", "score", "member_since", "state")
    )
    return df

def merge_dataframes(df1, df2):
    df1.withColumn("source", lit("US Softball League"))
    df2.withColumn("source", lit("Unity Golf Club"))

    df = df1.unionAll(df2)

    df = df.withColumn("id", row_number().over(Window.orderBy("member_since")))

    return df

if __name__ == "__main__":

    # Creating a Spark Session
    spark = SparkSession.builder \
        .appName("PySpark Example App") \
        .getOrCreate()

    # Creating unity_golf_club dataframe
    unity_golf_club_df = spark.read.csv("unity_golf_club.csv", header=True, inferSchema=True)

    # Creating us_softball_league dataframe
    us_softball_league_df = spark.read.csv("us_softball_league.tsv", sep = '\t', header=True, inferSchema=True)

    with open("us_state_abbr.json", "r") as file:
        us_state_abbr = json.load(file)
    state_map = create_map([lit(k) for k_v in us_state_abbr.items() for k in k_v])

    # Data Munging - step 1
    std_us_softball_league_df = standardize_df(us_softball_league_df, state_map)

    # Data Munging - step 2
    combined_df = merge_dataframes(std_us_softball_league_df, unity_golf_club_df)

    combined_df.show()

    # Stop the SparkSession
    spark.stop()
