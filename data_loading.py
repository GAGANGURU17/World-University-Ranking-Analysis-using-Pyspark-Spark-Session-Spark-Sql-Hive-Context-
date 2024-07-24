def load_data(spark, data_dir):
    """
    Load data from CSV files into Spark DataFrames.
    """
    times_df = spark.read.csv(f"{data_dir}/timesData.csv", header=True, inferSchema=True)
    cwur_df = spark.read.csv(f"{data_dir}/cwurData.csv", header=True, inferSchema=True)
    shanghai_df = spark.read.csv(f"{data_dir}/shanghaiData.csv", header=True, inferSchema=True)
    
    return {
        "times": times_df,
        "cwur": cwur_df,
        "shanghai": shanghai_df
    }
