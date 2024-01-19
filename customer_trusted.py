import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing to Trusted
CustomerLandingtoTrusted_node1705309151679 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={"paths": ["s3://meekbucket/landing/"], "recurse": True},
        transformation_ctx="CustomerLandingtoTrusted_node1705309151679",
    )
)

# Script generated for node SQL Query
SqlQuery932 = """
select * from myDataSource
where shareWithResearchAsOfDate
is not null
"""
SQLQuery_node1705309157389 = sparkSqlQuery(
    glueContext,
    query=SqlQuery932,
    mapping={"myDataSource": CustomerLandingtoTrusted_node1705309151679},
    transformation_ctx="SQLQuery_node1705309157389",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705309162488 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1705309157389,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://meekbucket/landing/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1705309162488",
)

job.commit()
