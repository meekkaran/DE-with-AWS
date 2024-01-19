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

# Script generated for node Accelerator Trusted
AcceleratorTrusted_node1705319454229 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://meekbucket/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AcceleratorTrusted_node1705319454229",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705319452846 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://meekbucket/landing/trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1705319452846",
)

# Script generated for node Join
Join_node1705320873353 = Join.apply(
    frame1=AcceleratorTrusted_node1705319454229,
    frame2=CustomerTrusted_node1705319452846,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1705320873353",
)

# Script generated for node Drop Fields
DropFields_node1705320879981 = DropFields.apply(
    frame=Join_node1705320873353,
    paths=[
        "`.customerName`",
        "`.email`",
        "`.phone`",
        "`.shareWithPublicAsOfDate`",
        "`.birthDay`",
        "`.lastUpdateDate`",
        "`.shareWithFriendsAsOfDate`",
        "`.registrationDate`",
        "`.serialNumber`",
        "`.shareWithResearchAsOfDate`",
    ],
    transformation_ctx="DropFields_node1705320879981",
)

# Script generated for node Customer Curated
CustomerCurated_node1705320886849 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1705320879981,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://meekbucket/landing/curated/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1705320886849",
)

job.commit()
