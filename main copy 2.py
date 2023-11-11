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
    "host": "teste.cxhbqq8ci3yq.us-east-1.rds.amazonaws.com",
    "database":"db_teste",
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

def save_row(row, cursor):
    conf = load_Conf()

    for item, config in conf.items():
        print(f"processando {item}")
        if config.get("id") in row:
            print(f"validou")

            for table in config.get("tables", []):

                cliente = [(row.__getitem__("objt_clie").__getitem__("nom_tipo_docm"))]
                
                tel = ["cod_tel_clie", "cod_area_tel", "cod_pais_tel", "num_tel_clie"]
                externalid = ["cliente"]
                key = ["num_tel_clie", "cliente"]  
                columns = [row.__getitem__("objt_clie").__getitem__("objt_tel_clie").__getitem__(f"{i}") for i in tel]
                colum_update = [f"""{i}='{row.__getitem__("objt_clie").__getitem__("objt_tel_clie").__getitem__(f"{i}")}'""" for i in tel]


                slq_statement = f"""
                        INSERT INTO public.tel_clie ({", ".join(tel + externalid)})
                        VALUES ('{"','".join(columns + cliente)}')
                        ON CONFLICT ({",".join(key)})
                        DO
                        UPDATE SET {",".join(colum_update)}
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
