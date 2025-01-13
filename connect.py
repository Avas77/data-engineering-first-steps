from pyspark.sql import SparkSession

jdbc_driver_path = "/home/avas/avas/postgres-jdbc/postgresql-42.7.4.jar"

# Database connection details
db_url = "jdbc:postgresql://localhost:5432/etl_pipeline"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def connect_postgres(table_name):
    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.driver.extraClassPath", jdbc_driver_path).getOrCreate()
    try:
        df = spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)
    except Exception as e:
        print("Error:", e)
    return df