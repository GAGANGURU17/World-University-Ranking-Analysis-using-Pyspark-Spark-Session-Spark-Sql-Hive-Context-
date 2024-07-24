from pyspark.sql.functions import col, avg, desc, when, collect_set

def analyze_times_data(df):
    """
    Analyze Times Higher Education World University Rankings data.
    """
    print("Top 10 universities in Times Higher Education Ranking:")
    df.filter(col("year") == 2016).orderBy("world_rank").limit(10).show()

    print("Average scores by country in 2016:")
    df.filter(col("year") == 2016).groupBy("country").agg(avg("total_score").alias("avg_score")) \
        .orderBy(desc("avg_score")).show()

def analyze_cwur_data(df):
    """
    Analyze Center for World University Rankings (CWUR) data.
    """
    print("Top 10 universities in CWUR Ranking:")
    df.filter(col("year") == 2015).orderBy("world_rank").limit(10).show()

    print("Universities with high alumni employment:")
    df.filter(col("year") == 2015).orderBy(desc("alumni_employment")).limit(10).show()

def analyze_shanghai_data(df):
    """
    Analyze Academic Ranking of World Universities (Shanghai Ranking) data.
    """
    print("Top 10 universities in Shanghai Ranking:")
    df.filter(col("year") == 2015).orderBy("world_rank").limit(10).show()

    print("Universities with high research output:")
    df.filter(col("year") == 2015).orderBy(desc("pcp")).limit(10).show()

def compare_rankings(dataframes):
    """
    Compare rankings across different systems.
    """
    times_top = set(dataframes["times"].filter(col("year") == 2015).orderBy("world_rank").limit(100).select("university_name").rdd.flatMap(lambda x: x).collect())
    cwur_top = set(dataframes["cwur"].filter(col("year") == 2015).orderBy("world_rank").limit(100).select("institution").rdd.flatMap(lambda x: x).collect())
    shanghai_top = set(dataframes["shanghai"].filter(col("year") == 2015).orderBy("world_rank").limit(100).select("university_name").rdd.flatMap(lambda x: x).collect())

    common_universities = times_top.intersection(cwur_top).intersection(shanghai_top)
    print(f"Universities in Top 100 of all three rankings: {len(common_universities)}")
    for uni in list(common_universities)[:10]:
        print(uni)
