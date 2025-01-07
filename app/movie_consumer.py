from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

scala_version = '2.12'
spark_version = '3.2.3'

MASTER = os.environ["MASTER"]
KAFKA_BROKER1 = os.environ["KAFKA_BROKER1"]
MOVIE_TOPIC = os.environ["MOVIE_TOPIC"]
ES_NODES = os.environ['ES_NODES']
USERNAME = os.environ['USERNAME_ATLAS']
PASSWORD = os.environ['PASSWORD_ATLAS']
CONNECTION_STRING = os.environ['CONNECTION_STRING']
ES_RESOURCE = "movie"

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.5.0',
    'org.apache.hadoop:hadoop-client:3.2.0',
    'org.elasticsearch:elasticsearch-spark-30_2.12:7.17.16',
    "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
]

spark = SparkSession.builder \
    .master(MASTER) \
    .appName("Movie Consumer") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.mongodb.input.uri", CONNECTION_STRING) \
    .config("spark.mongodb.output.uri", CONNECTION_STRING) \
    .config("spark.cores.max", "1") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("Error")
