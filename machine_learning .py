import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step trainer trusted
steptrainertrusted_node1705666935306 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://meekbucket/step_trainer/landing/trusted/"]},
    transformation_ctx="steptrainertrusted_node1705666935306",
)

# Script generated for node customer curated
customercurated_node1705666936357 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://meekbucket/landing/curated/"]},
    transformation_ctx="customercurated_node1705666936357",
)

# Script generated for node Join
Join_node1705666941021 = Join.apply(
    frame1=customercurated_node1705666936357,
    frame2=steptrainertrusted_node1705666935306,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1705666941021",
)

# Script generated for node Drop Fields
DropFields_node1705667727285 = DropFields.apply(
    frame=Join_node1705666941021,
    paths=[
        "`.email`",
        "`.phone`",
        "`.lastupdatedate`",
        "`.serialnumber`",
        "`.sharewithfriendsasofdate`",
        "`.customername`",
        "`.registrationdate`",
        "`.sharewithresearchasofdate`",
        "`.sharewithpublicasofdate`",
        "`.birthday`",
    ],
    transformation_ctx="DropFields_node1705667727285",
)

# Script generated for node Amazon S3
AmazonS3_node1705666944248 = glueContext.getSink(
    path="s3://meekbucket/step_trainer/machine_learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1705666944248",
)
AmazonS3_node1705666944248.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning"
)
AmazonS3_node1705666944248.setFormat("json")
AmazonS3_node1705666944248.writeFrame(DropFields_node1705667727285)
job.commit()
