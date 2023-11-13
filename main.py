from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import boto3
import psycopg2
from pyspark import SparkConf
import json
from pyspark.sql import SQLContext, Row
import os
import yaml

session = boto3.session.Session(profile_name="mkt-data")
sts_connection = session.client("sts")
response = sts_connection.get_session_token(DurationSeconds=3600)
credentials = response["Credentials"]

conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
conf.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
)
conf.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
)
conf.set("spark.jars", "postgresql-42.5.4.jar")
conf.set("spark.hadoop.fs.s3a.access.key", credentials["AccessKeyId"])
conf.set("spark.hadoop.fs.s3a.secret.key", credentials["SecretAccessKey"])
conf.set("spark.hadoop.fs.s3a.session.token", credentials["SessionToken"])

spark = (
    SparkSession.builder.master("local[2]")
    .appName("my_app")
    .config(conf=conf)
    .enableHiveSupport()
    .getOrCreate()
)


sc = spark.sparkContext

ssc = StreamingContext(sc, 5)

sqlContext = SQLContext(sc)

stream = ssc.textFileStream("s3a://mktplace-landing-zone/")

from pyspark.sql import functions as F
from pyspark.sql import types as T

prop = {
    "driver": "org.postgresql.Driver",
    "host": "teste1.cxhbqq8ci3yq.us-east-1.rds.amazonaws.com",
    "database":"teste_db",
    "user": "teste",
    "password": "teste123",
}

connect = sc.broadcast(prop)

def load_Conf():
    file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "pedidos.yaml"
        )
    with open(file_path, "r") as f:
        conf = yaml.safe_load(f)
        return conf

def get_item(row, key):
    if len(key) == 1:
        return row[f"{key[0]}"]
    else:
        return row[f"{key[0]}"][f"{key[1]}"]


def save_row(row, cursor):
    conf = load_Conf()

    for item, config in conf.items():
        print(f"processando {item}")
        if config.get("id") in row:
            for table in config.get("tables", []):
                print(f"salvando dados de {table['name']}")

                dicta = row.asDict([True])
                
                source = table.get("source_keys", [])
                data = get_item(dicta, source) if source else dicta

                external = table.get("external_source_values", [])
                data2 = [get_item(dicta, external)] if external else []

                external_update = [f"""{i}='{data2[0]}'""" for i in table.get("external_keys", [])]

                for item in [data]:
                    print(item)
                              
                    columns = [data.__getitem__(f"{i}") for i in table["fields"]]
                    colum_update = [f"""{i}='{data.__getitem__(f"{i}")}'""" for i in table["fields"]]


                    slq_statement = f"""
                            INSERT INTO {table["name"]} ({", ".join(table["fields"] + table.get("external_keys", []))})
                            VALUES ('{"','".join(columns + data2)}')
                            ON CONFLICT ({",".join(table["table_pk"])})
                            DO
                            UPDATE SET {",".join(colum_update + external_update)}
                        """

                    cursor.execute(slq_statement)

def save_data(partition):
    conn = connect.value

    db_conn = psycopg2.connect(
        host = conn.get("host"),
        user = conn.get("user"),
        password = conn.get("password"),
        database = conn.get("database"),
    )

    cursor = db_conn.cursor()

    for row in partition:
        save_row(row, cursor)

    db_conn.commit()
    cursor.close()
    db_conn.close()


def readMyStream(rdd):
    if not rdd.isEmpty():
        print("Started the Process")
        str_json = rdd.reduce(lambda x, y: x + y)
        y = sc.parallelize([str_json])
        df = spark.read.json(y)

        df.foreachPartition(save_data)

stream.foreachRDD(lambda rdd: readMyStream(rdd))
ssc.start()
ssc.awaitTermination()
