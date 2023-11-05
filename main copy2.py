from pyspark import SparkConf
from pyspark.sql import SparkSession
import boto3

session = boto3.session.Session(profile_name='mkt-data')
sts_connection = session.client('sts')
response = sts_connection.get_session_token(DurationSeconds=3600)
credentials = response['Credentials']

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
conf.set("spark.hadoop.fs.s3a.access.key", credentials['AccessKeyId'])
conf.set("spark.hadoop.fs.s3a.secret.key", credentials['SecretAccessKey'])
conf.set("spark.hadoop.fs.s3a.session.token", credentials['SessionToken'])
 
spark = SparkSession.builder.config(conf=conf).getOrCreate()
 
df = spark.read.csv('s3a://mktplace-landing-zone/8rQeIH1kGo6bV9c5_1.json', inferSchema=True)

df.show()