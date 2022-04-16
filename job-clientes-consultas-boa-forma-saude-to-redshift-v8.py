import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="banco-documento-boa-forma-saude",
    table_name="data_storage_receptor_documentos",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1649975631170 = glueContext.create_dynamic_frame.from_catalog(
    database="banco-web-service-boa-forma-saude",
    table_name="data_storage_receptor_web_services",
    transformation_ctx="AmazonS3_node1649975631170",
)

# Script generated for node Join
AmazonS3_node1649975631170DF = AmazonS3_node1649975631170.toDF()
S3bucket_node1DF = S3bucket_node1.toDF()
Join_node1649975645013 = DynamicFrame.fromDF(
    AmazonS3_node1649975631170DF.join(
        S3bucket_node1DF,
        (
            AmazonS3_node1649975631170DF["carteirinhaid"]
            == S3bucket_node1DF["carteirinha"]
        ),
        "outer",
    ),
    glueContext,
    "Join_node1649975645013",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Join_node1649975645013,
    mappings=[
        ("cpf", "string", "cpf", "string"),
        ("datanascimento", "string", "datanascimento", "string"),
        ("carteirinha", "long", "carteirinha", "long"),
        ("nome", "string", "nome", "string"),
        ("valor", "long", "valor", "long"),
        ("especialização", "string", "especialização", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Redshift Cluster
RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="banco-redshift-boa-forma-saude",
    table_name="dev_public_clientes_boa_forma",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="RedshiftCluster_node3",
)

job.commit()
