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

# Script generated for node Customer curated
Customercurated_node1705322309193 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://meekbucket/landing/curated/"], "recurse": True},
    transformation_ctx="Customercurated_node1705322309193",
)

# Script generated for node Step Trainer
StepTrainer_node1705322308173 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://meekbucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainer_node1705322308173",
)

# Script generated for node Join
Join_node1705322312567 = Join.apply(
    frame1=Customercurated_node1705322309193,
    frame2=StepTrainer_node1705322308173,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1705322312567",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1705322320473 = glueContext.getSink(
    path="s3://meekbucket/step_trainer/landing/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="steptrainertrusted_node1705322320473",
)
steptrainertrusted_node1705322320473.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
steptrainertrusted_node1705322320473.setFormat("json")
steptrainertrusted_node1705322320473.writeFrame(Join_node1705322312567)
job.commit()
