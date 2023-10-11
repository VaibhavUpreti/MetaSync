from kafka import KafkaProducer, KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster

def add_db_stream(database_url):
    if "postgres" in database_url:
        db_type = "PostgreSQL"
        kafka_topic = "postgres_topic"
    elif "redis" in database_url:
        db_type = "Redis"
        kafka_topic = "redis_topic"
    elif "mysql" in database_url:
        db_type = "MySQL"
        kafka_topic = "mysql_topic"
    else:
        raise ValueError("Unsupported database type")

    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Create a Kafka topic for the identified database
    producer.send(kafka_topic, key=b'db_type', value=db_type.encode('utf-8'))

    # Initialize a Spark Streaming context
    sc = SparkContext(appName="DBStreamApp")
    ssc = StreamingContext(sc, 1)  # 1-second batch interval

    # Create a Kafka stream
    kafka_stream = KafkaUtils.createStream(
        ssc, 'localhost:2181', 'db_stream_group', {kafka_topic: 1}
    )

    # Process the Kafka stream (you can define your processing logic here)
    kafka_stream.map(lambda x: process_data(x[1]))

    # Start the Spark Streaming application
    ssc.start()
    ssc.awaitTermination()

def process_data(data):
    # Define your data processing logic here
    # For example, you can write the data to Cassandra
    cluster = Cluster(['localhost'])
    session = cluster.connect('your_keyspace')
    session.execute('INSERT INTO your_table (column1, column2) VALUES (?, ?)', (data['column1'], data['column2']))
    cluster.shutdown()

# Example usage:
db_url = "postgresql://postgres:postgres@localhost:5432/circuitverse_production"
add_db_stream(db_url)



