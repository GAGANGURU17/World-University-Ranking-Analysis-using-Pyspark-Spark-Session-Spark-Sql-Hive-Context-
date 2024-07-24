import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.analysis import analyze_times_data, analyze_cwur_data, analyze_shanghai_data

class TestAnalysis(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestWorldUniversityRankingAnalysis") \
            .getOrCreate()
        
        # Create sample DataFrames for testing
        times_data = [
            (2016, "University A", "USA", 1, 96.1),
            (2016, "University B", "UK", 2, 94.3),
            (2016, "University C", "USA", 3, 93.7),
        ]
        times_columns = ["year", "university_name", "country", "world_rank", "total_score"]
        cls.times_df = cls.spark.createDataFrame(times_data, times_columns)

        cwur_data = [
            (2015, "University A", 1, 100),
            (2015, "University B", 2, 98),
            (2015, "University C", 3, 97),
        ]
        cwur_columns = ["year", "institution", "world_rank", "alumni_employment"]
        cls.cwur_df = cls.spark.createDataFrame(cwur_data, cwur_columns)

        shanghai_data = [
            (2015, "University A", 1, 100),
            (2015, "University B", 2, 98),
            (2015, "University C", 3, 97),
        ]
        shanghai_columns = ["year", "university_name", "world_rank", "pcp"]
        cls.shanghai_df = cls.spark.createDataFrame(shanghai_data, shanghai_columns)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_analyze_times_data(self):
        result = analyze_times_data(self.times_df)
        
        # Test top universities
        top_unis = result["top_universities"].collect()
        self.assertEqual(len(top_unis), 3)
        self.assertEqual(top_unis[0]["university_name"], "University A")
        self.assertEqual(top_unis[0]["world_rank"], 1)

        # Test average scores
        avg_scores = result["avg_scores"].collect()
        self.assertEqual(len(avg_scores), 2)  # Two countries in our sample data
        usa_score = next(score for score in avg_scores if score["country"] == "USA")
        self.assertAlmostEqual(usa_score["avg_score"], 94.9, places=1)

    def test_analyze_cwur_data(self):
        result = analyze_cwur_data(self.cwur_df)
        
        # Test top universities
        top_unis = result["top_universities"].collect()
        self.assertEqual(len(top_unis), 3)
        self.assertEqual(top_unis[0]["institution"], "University A")
        self.assertEqual(top_unis[0]["world_rank"], 1)

        # Test alumni employment
        top_employment = result["top_employment"].collect()
        self.assertEqual(len(top_employment), 3)
        self.assertEqual(top_employment[0]["institution"], "University A")
        self.assertEqual(top_employment[0]["alumni_employment"], 100)

    def test_analyze_shanghai_data(self):
        result = analyze_shanghai_data(self.shanghai_df)
        
        # Test top universities
        top_unis = result["top_universities"].collect()
        self.assertEqual(len(top_unis), 3)
        self.assertEqual(top_unis[0]["university_name"], "University A")
        self.assertEqual(top_unis[0]["world_rank"], 1)

        # Test research output
        top_research = result["top_research"].collect()
        self.assertEqual(len(top_research), 3)
        self.assertEqual(top_research[0]["university_name"], "University A")
        self.assertEqual(top_research[0]["pcp"], 100)

if __name__ == '__main__':
    unittest.main()
