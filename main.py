from pyspark.sql import SparkSession
from data_loading import load_data
from analysis import (
    analyze_times_data,
    analyze_cwur_data,
    analyze_shanghai_data,
    compare_rankings
)

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("World University Ranking Analysis") \
        .enableHiveSupport() \
        .getOrCreate()

    # Load data
    dataframes = load_data(spark, "data")

    # Perform analyses
    analyze_times_data(dataframes["times"])
    analyze_cwur_data(dataframes["cwur"])
    analyze_shanghai_data(dataframes["shanghai"])
    compare_rankings(dataframes)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
