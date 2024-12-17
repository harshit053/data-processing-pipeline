from pyspark.sql import SparkSession

from utils import load_state_map, merge_dataframes, replace_company_id, standardize_df, validate_df


def solution():
    # Creating a Spark Session
    spark = SparkSession.builder.appName("PySpark App").getOrCreate()

    # Creating unity_golf_club dataframe
    unity_golf_club_df = spark.read.csv(
        "unity_golf_club.csv", header=True, inferSchema=True
    )

    # Creating us_softball_league dataframe
    us_softball_league_df = spark.read.csv(
        "us_softball_league.tsv", sep="\t", header=True, inferSchema=True
    )

    # Creating companies dataframe
    companies_df = spark.read.csv("companies.csv", header=True, inferSchema=True)

    state_map = load_state_map()

    std_us_softball_league_df = standardize_df(us_softball_league_df, state_map)

    combined_df = merge_dataframes(std_us_softball_league_df, unity_golf_club_df)

    combined_df = replace_company_id(combined_df, companies_df)

    suspect_df, valid_df = validate_df(combined_df)

    valid_df.show()

    suspect_df.show()

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":

    solution()
