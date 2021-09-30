## World University Ranking Analysis: Using Pyspark, Spark Session, Spark SQL, Hive Context
    
* Building Spark Session.
* Loading the data from CSV to Data Frame.
* Query On loaded data.
* Creating Global View and retrieving data from view.
* Creating Temporary View and retrieving data from view.
* Location of Temporary Viewed data stored. 
* Stored Temporary View Queried Data.
* Importing Hive Context and performing a query on Hive table.
* Using if condition checking whether the national_rank is 1 or not.
* Partitioning the column and displaying partitioned data. 
* Using the collect_set function to display distinct values in a column.

## Technologies Used
* HDFS - v2.7
* Spark - v2.3
* Hive - v1.2

## Features
* Created Spark_Dataframe and implemented Spark_Dataframes methods.
* Created temporary Table in Spark and applied some Spark_SQL queries on it.
* Did partitioning and bucketing with the help of Spark_SQL.
* Connected Hive To Spark.

## Getting Started
#### importing some packages

 * import pyspark                                                                                                                                   
 * from pyspark.sql import sparksession 


#### Creating Dataframe in spark

* df2 = spark.read.csv('/project2/test_data.csv',inferSchema=True,header=False).toDF("id","Hcode","HCcode","A_Room","Dep","Wcode","Bed_Grade","pid"
,"PCcode","Admission","Illness","Visit_P","P_age","Deposit") 

#### Creting Temporary Table in Spark

* df3=df2.createOrReplaceTempView("Health_Care")

## Contributors
* Gagan N
* ventaka reddy kavori
* venkata naga sai mulkutla
* yashwant vuppulapti


## References
https://sparkbyexamples.com/spark/different-ways-to-create-a-spark-dataframe/
https://sparkbyexamples.com/spark/spark-sqlcontext-explained-with-examples/
https://spark.apache.org/docs/latest/sql-programming-guide.html

https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
