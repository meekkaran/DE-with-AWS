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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1705318155673 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://meekbucket/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1705318155673",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705318157116 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://meekbucket/landing/trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1705318157116",
)

# Script generated for node Join
Join_node1705318214585 = Join.apply(
    frame1=AccelerometerLanding_node1705318155673,
    frame2=CustomerTrusted_node1705318157116,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1705318214585",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705318219587 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1705318214585,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://meekbucket/accelerometer/trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AccelerometerTrusted_node1705318219587",
)

job.commit()
