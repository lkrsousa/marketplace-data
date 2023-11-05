
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import boto3
from pyspark.sql.functions import col
from pyspark import SparkConf
import json

session = boto3.session.Session(profile_name='mkt-data')
sts_connection = session.client('sts')
response = sts_connection.get_session_token(DurationSeconds=3600)
credentials = response['Credentials']

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
# conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
conf.set("spark.jars", "com.amazon.redshift.jdbc42.Driver")
conf.set("spark.hadoop.fs.s3a.access.key", credentials['AccessKeyId'])
conf.set("spark.hadoop.fs.s3a.secret.key", credentials['SecretAccessKey'])
conf.set("spark.hadoop.fs.s3a.session.token", credentials['SessionToken'])

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("my_app") \
    .config(conf=conf) \
    .getOrCreate()


sc = spark.sparkContext

ssc = StreamingContext(sc, 5)

stream = ssc.textFileStream('s3a://mktplace-landing-zone/')

def readMyStream(rdd):
  if not rdd.isEmpty():
    print('Started the Process')
    str_json = rdd.reduce(lambda x, y: x + y)
    y = sc.parallelize([str_json])
    df = spark.read.json(y)

    if "txt_detl_idt_pedi_pgto" in  df.columns:
      df.printSchema()
      df.show()
      # df.write.jdbc(url="jdbc:redshift://dev.458270960978.us-east-1.redshift-serverless.amazonaws.com:5439/dev?user=dev&password=desenvolvimento",
      #   table="raw_pedidos",
      #   properties=redshift_properties,
      #   mode="overwrite",
      # )
      # from pyspark.sql import SQLContext
      # sql_context = SQLContext(sc)
      # redshift_url="jdbc:redshift://dev.458270960978.us-east-1.redshift-serverless.amazonaws.com:5439/dev?user=dev&password=desenvolvimento"
      # df_users = sql_context.read \
      #     .format("com.amazon.redshift.jdbc42.Driver") \
      #     .option("url", redshift_url) \
      #     .option("query", "select * from raw_pedidos") \
      #     .option("tempdir", "s3a://mktplace-temp-zone/") \
      #     .load()
      
      # df_users.show()


stream.foreachRDD( lambda rdd: readMyStream(rdd) )
ssc.start()
ssc.awaitTermination()
