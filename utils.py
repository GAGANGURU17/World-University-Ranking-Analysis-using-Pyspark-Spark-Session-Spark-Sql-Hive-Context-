from pyspark.sql.functions import col

def filter_by_year(df, year):
    """
    Filter DataFrame by a specific year.
    """
    return df.filter(col("year") == year)

def calculate_average_score(df, score_column):
    """
    Calculate average score for a given score column.
    """
    return df.agg({score_column: "avg"}).collect()[0][0]

def rank_universities(df, rank_column):
    """
    Rank universities based on a specific column.
    """
    return df.orderBy(col(rank_column).asc())
