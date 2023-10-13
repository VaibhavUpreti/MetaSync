from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import os
import uuid
import time

os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1 pyspark-shell'

def create_spark_session():
    spark_session = SparkSession.builder \
        .appName('MetaStreaming') \
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                       "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
        .config('spark.cassandra.connection.host', '0.0.0.0') \
        .master("local[*]") \
        .getOrCreate()

    return spark_session 

def add_kafka_streams(spark_session):
    df = spark_session \
      .readStream \
      .format("kafka") \
      .option('kafka.bootstrap.servers', '0.0.0.0:9092') \
      .option("subscribe", "postgres.public.users") \
      .option("startingOffsets", "earliest") \
      .load()

    return df

def create_selection_df(df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("email", StringType(), False),
        StructField("name", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("updated_at", StringType(), False),
        StructField("provider", StringType(), False),
        StructField("uid", StringType(), False),
        StructField("admin", StringType(), False),
        StructField("country", StringType(), False),
        StructField("educational_institute", StringType(), False),
        StructField("subscribed", StringType(), False),
        StructField("locale", StringType(), False)
    ])
    
    sel = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)
    return sel


def create_cassandra_session():
    cluster = Cluster(['0.0.0.0'])
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS circuitverse 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    return session

def initialize_workspace(session):

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS circuitverse 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS circuitverse.users (
            id UUID PRIMARY KEY,
            email TEXT,
            name TEXT,
            created_at TEXT,
            updated_at TEXT,
            provider TEXT,
            uid TEXT,
            admin TEXT,
            country TEXT,
            educational_institute TEXT,
            subscribed TEXT,
            locale TEXT
        );
    """)

def insert_data(session, **kwargs):
    print("Inserting user data...")


    # user_id = uuid.uuid4()
    user_id = kwargs.get('id')
    email = kwargs.get('email')
    name = kwargs.get('name')
    created_at = kwargs.get('created_at')
    updated_at = kwargs.get('updated_at')
    provider = kwargs.get('provider', 'em')
    uid = kwargs.get('uid', 'wer')
    admin = kwargs.get('admin', 'rewr')
    country = kwargs.get('country', 'rewr')
    educational_institute = kwargs.get('educational_institute', 'rwer')
    subscribed = kwargs.get('subscribed', 'res')
    locale = kwargs.get('locale', 'en')

    time.sleep(200)
    session.execute("""
        INSERT INTO circuitverse.users(id, email, name, created_at, updated_at,
            provider, uid, admin, country, educational_institute, subscribed, locale)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, ( user_id, email, name, created_at, updated_at, provider, uid, admin, country, educational_institute, subscribed, locale))
    print(f"Data inserted for user with ID {user_id}")


if __name__ == "__main__":

    print("Creating Spark session")
    spark_session = create_spark_session()
    print("Spark Session Running....")
    if spark_session is not None:
        df = add_kafka_streams(spark_session)
        selection_df = create_selection_df(df)
        session = create_cassandra_session()
        print("Cassandra Session Created....")
        if session is not None:
            initialize_workspace(session)
            selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
               .foreachBatch(insert_data(session)) \
               .option('checkpointLocation', '/Users/vaibhavupreti/Desktop/MetaSync/metasync/tmp/checkpoint/') \
               .option('keyspace', 'circuitverse') \
               .option('table', 'users') \
               .trigger(processingTime="1 minutes") \
               .start()#.awaitTermination()
