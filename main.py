from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import boto3
from pyspark.sql.functions import col
from pyspark import SparkConf
import json
from pyspark.sql import SQLContext, Row

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


def readMyStream(rdd):
    if not rdd.isEmpty():
        print("Started the Process")
        str_json = rdd.reduce(lambda x, y: x + y)
        y = sc.parallelize([str_json])
        df = spark.read.json(y)

        if "txt_detl_idt_pedi_pgto" in df.columns:

            id_pedido = df.select("txt_detl_idt_pedi_pgto").withColumnRenamed("txt_detl_idt_pedi_pgto", "pedido")

            client = [
                "nom_tipo_docm",
                "nom_ende_emai_clie",
                "nom_clie",
                "cod_rfrc_clie",
                "cod_tipo_pess",
                "objt_tel_clie",
            ]

            clie = df.select(*[F.col("objt_clie")[i].alias(f"{i}") for i in client])

            id_cliente = clie.select("nom_tipo_docm").withColumnRenamed("nom_tipo_docm", "cliente")

            pedido = df.select(
                "cod_aces_tokn",
                "cod_idef_clie",
                "dat_atui",
                "dat_hor_tran",
                "idt_loja",
                "num_ulti_vers_sist",
                "stat_pedi",
                "stat_pgto",
                "txt_detl_idt_pedi_pgto",
            ).crossJoin(id_cliente)

            objt_clie = clie.select(
                "nom_tipo_docm",
                "nom_ende_emai_clie",
                "nom_clie",
                "cod_rfrc_clie",
                "cod_tipo_pess",
            )

            tel = ["cod_tel_clie", "cod_area_tel", "cod_pais_tel", "num_tel_clie"]

            tel_clie = clie.select(
                *[F.col("objt_tel_clie")[i].alias(f"{i}") for i in tel]
            )

            objt_tel_clie = tel_clie.crossJoin(id_cliente)

            end_entr_fields = [
                "nom_cida_ende_entg",
                "nom_cmpl_ende_entg",
                "cod_pais_resd",
                "nom_bair_ende_entg",
                "num_logr_ende_entg",
                "nom_pess",
                "sig_uf",
                "nom_estd",
                "nom_logr_ende_entg",
                "cod_cep",
            ]

            end_entr = df.select(
                *[
                    F.col("objt_ende_entg_pedi")[i].alias(f"{i}")
                    for i in end_entr_fields
                ]
            )

            objt_ende_entg_pedi = end_entr.crossJoin(id_cliente)

            list_desc_item_pedi = df.select("list_desc_item_pedi")

            list_item_fields = [
                "des_moda_entg_item",
                "qtd_dia_prz_entg",
                "vlr_totl_fret",
                "nom_oped_empr",
                "nom_item",
                "cod_ofrt_ineo_requ",
                "vlr_oril",
                "vlr_prod",
                "vlr_prod_ofrt_desc",
                "nom_prod_orig",
                "txt_plae_otmz_url_prod",
                "qtd_item_pedi",
                "idt_venr",
                "nom_venr",
                "idt_vrne_venr",
                "vlr_fret",
                "vlr_desc_envo",
                "cod_vrne_prod",
                "txt_plae_otmz_url_item",
                "nom_url_img_oril",
                "vlr_oril_prod_unit_vrne",
                "vlr_prec_unit_brut",
                "list_cod_loja_vend",
            ]
            list_item = df.select(
                *[F.col("list_item_pedi")[i].alias(f"{i}") for i in list_item_fields]
            ).crossJoin(id_pedido)

            list_item_data = list_item.select(
                "des_moda_entg_item",
                "qtd_dia_prz_entg",
                "vlr_totl_fret",
                "nom_oped_empr",
                "nom_item",
                "cod_ofrt_ineo_requ",
                "vlr_oril",
                "vlr_prod",
                "vlr_prod_ofrt_desc",
                "nom_prod_orig",
                "txt_plae_otmz_url_prod",
                "qtd_item_pedi",
                "idt_venr",
                "nom_venr",
                "idt_vrne_venr",
                "vlr_fret",
                "vlr_desc_envo",
                "cod_vrne_prod",
                "txt_plae_otmz_url_item",
                "nom_url_img_oril",
                "vlr_oril_prod_unit_vrne",
                "vlr_prec_unit_brut",
            ).crossJoin(id_pedido)

            loja_fields = ["cod_loja_vend", "qtd_loja_item"]

            qtd_item = list_item.select(
                F.size("list_cod_loja_vend").alias("count")
            ).collect()[0][0]

            list_cod_loja_vend = (
                list_item.select(
                    *[
                        [
                            F.col("list_cod_loja_vend")[0][i].alias(f"{i}")
                            for i in loja_fields
                        ]
                        for i in range(qtd_item)
                    ]
                )
                .crossJoin(id_pedido)
            )

            list_item = df.select("list_envo")

            objt_des_rsum_fields = [
                "num_item",
                "qtd_maxi_temp_envo",
                "vlr_desc_conc_pedi",
                "vlr_totl_desc",
                "vlr_cust_totl_envo",
                "vlr_desc_totl_envo",
            ]

            objt_des_rsum = df.select(
                *[F.col("objt_des_rsum")[i].alias(f"{i}") for i in objt_des_rsum_fields]
            ).crossJoin(pedido)

        prop = {
            "driver": "org.postgresql.Driver",
            "user": "db_teste",
            "password": "teste123",
        }

        pedido.write.jdbc(
            "jdbc:postgresql://teste.cxhbqq8ci3yq.us-east-1.rds.amazonaws.com:5432/db_teste?user=teste&password=teste123",
            table="public.raw_pedidos",
            mode="append",
            properties=prop,
        )

        pedido.write.jdbc(
            "jdbc:postgresql://teste.cxhbqq8ci3yq.us-east-1.rds.amazonaws.com:5432/db_teste?user=teste&password=teste123",
            table="public.pedidos",
            mode="append",
            properties=prop,
        )

        try:
            objt_clie.write.jdbc(
                "jdbc:postgresql://teste.cxhbqq8ci3yq.us-east-1.rds.amazonaws.com:5432/db_teste?user=teste&password=teste123",
                table="public.cliente",
                mode="append",
                properties=prop,
            )
        except NameError:
            print(NameError)
        

        objt_tel_clie.write.jdbc(
            "jdbc:postgresql://teste.cxhbqq8ci3yq.us-east-1.rds.amazonaws.com:5432/db_teste?user=teste&password=teste123",
            table="public.tel_clie",
            mode="append",
            properties=prop,
        )

        objt_ende_entg_pedi.write.jdbc(
            "jdbc:postgresql://teste.cxhbqq8ci3yq.us-east-1.rds.amazonaws.com:5432/db_teste?user=teste&password=teste123",
            table="public.endereco",
            mode="append",
            properties=prop,
        )

        list_item_data.write.jdbc(
            "jdbc:postgresql://teste.cxhbqq8ci3yq.us-east-1.rds.amazonaws.com:5432/db_teste?user=teste&password=teste123",
            table="public.items",
            mode="append",
            properties=prop,
        )

        objt_des_rsum.write.jdbc(
            "jdbc:postgresql://teste.cxhbqq8ci3yq.us-east-1.rds.amazonaws.com:5432/db_teste?user=teste&password=teste123",
            table="public.resumo",
            mode="append",
            properties=prop,
        )


stream.foreachRDD(lambda rdd: readMyStream(rdd))
ssc.start()
ssc.awaitTermination()
