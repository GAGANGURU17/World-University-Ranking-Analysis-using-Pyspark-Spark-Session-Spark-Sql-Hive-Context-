from pyspark.sql.functions import col, avg, desc, when, collect_set

def analyze_times_data(df):
    """
    Analyze Times Higher Education World University Rankings data.
    """
    top_universities = df.filter(col("year") == 2016).orderBy("world_rank").limit(10)
    
    avg_scores = df.filter(col("year") == 2016).groupBy("country") \
        .agg(avg("total_score").alias("avg_score")) \
        .orderBy(desc("avg_score"))

    return {
        "top_universities": top_universities,
        "avg_scores": avg_scores
    }

def analyze_cwur_data(df):
    """
    Analyze Center for World University Rankings (CWUR) data.
    """
    top_universities = df.filter(col("year") == 2015).orderBy("world_rank").limit(10)

    top_employment = df.filter(col("year") == 2015) \
        .orderBy(desc("alumni_employment")).limit(10)

    return {
        "top_universities": top_universities,
        "top_employment": top_employment
    }

def analyze_shanghai_data(df):
    """
    Analyze Academic Ranking of World Universities (Shanghai Ranking) data.
    """
    top_universities = df.filter(col("year") == 2015).orderBy("world_rank").limit(10)

    top_research = df.filter(col("year") == 2015).orderBy(desc("pcp")).limit(10)

    return {
        "top_universities": top_universities,
        "top_research": top_research
    }

def compare_rankings(dataframes):
    """
    Compare rankings across different systems.
    """
    times_top = set(dataframes["times"].filter(col("year") == 2015).orderBy("world_rank").limit(100).select("university_name").rdd.flatMap(lambda x: x).collect())
    cwur_top = set(dataframes["cwur"].filter(col("year") == 2015).orderBy("world_rank").limit(100).select("institution").rdd.flatMap(lambda x: x).collect())
    shanghai_top = set(dataframes["shanghai"].filter(col("year") == 2015).orderBy("world_rank").limit(100).select("university_name").rdd.flatMap(lambda x: x).collect())

    common_universities = times_top.intersection(cwur_top).intersection(shanghai_top)
    
    return {
        "common_universities": list(common_universities),
        "common_count": len(common_universities)
    }

def analyze_international_outlook(df):
    """
    Analyze international outlook scores from Times Higher Education data.
    """
    avg_international_outlook = df.filter(col("year") == 2016) \
        .groupBy("country") \
        .agg(avg("international_outlook").alias("avg_international_outlook")) \
        .orderBy(desc("avg_international_outlook"))

    top_international_universities = df.filter(col("year") == 2016) \
        .orderBy(desc("international_outlook")).limit(10)

    return {
        "avg_international_outlook": avg_international_outlook,
        "top_international_universities": top_international_universities
    }

def analyze_research_impact(df):
    """
    Analyze research impact (citations) from Times Higher Education data.
    """
    avg_citations = df.filter(col("year") == 2016) \
        .groupBy("country") \
        .agg(avg("citations").alias("avg_citations")) \
        .orderBy(desc("avg_citations"))

    top_research_universities = df.filter(col("year") == 2016) \
        .orderBy(desc("citations")).limit(10)

    return {
        "avg_citations": avg_citations,
        "top_research_universities": top_research_universities
    }

def analyze_teaching_quality(df):
    """
    Analyze teaching quality from Times Higher Education data.
    """
    avg_teaching = df.filter(col("year") == 2016) \
        .groupBy("country") \
        .agg(avg("teaching").alias("avg_teaching")) \
        .orderBy(desc("avg_teaching"))

    top_teaching_universities = df.filter(col("year") == 2016) \
        .orderBy(desc("teaching")).limit(10)

    return {
        "avg_teaching": avg_teaching,
        "top_teaching_universities": top_teaching_universities
    }

def analyze_trends(df, column):
    """
    Analyze trends over years for a specific metric.
    """
    trend = df.groupBy("year") \
        .agg(avg(column).alias(f"avg_{column}")) \
        .orderBy("year")

    return trend

def analyze_country_performance(df):
    """
    Analyze overall country performance in Times Higher Education rankings.
    """
    country_performance = df.filter(col("year") == 2016) \
        .groupBy("country") \
        .agg(
            avg("total_score").alias("avg_total_score"),
            avg("teaching").alias("avg_teaching"),
            avg("research").alias("avg_research"),
            avg("citations").alias("avg_citations"),
            avg("international_outlook").alias("avg_international_outlook")
        ) \
        .orderBy(desc("avg_total_score"))

    return country_performance
