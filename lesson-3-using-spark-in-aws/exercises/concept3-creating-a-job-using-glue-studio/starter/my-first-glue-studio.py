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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1726315367449 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://andrzul-data-lake/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1726315367449")

# Script generated for node Share With Research
SqlQuery1014 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
ShareWithResearch_node1726315709697 = sparkSqlQuery(glueContext, query = SqlQuery1014, mapping = {"myDataSource":CustomerLanding_node1726315367449}, transformation_ctx = "ShareWithResearch_node1726315709697")

# Script generated for node Customer Trusted
CustomerTrusted_node1726315919322 = glueContext.write_dynamic_frame.from_options(frame=ShareWithResearch_node1726315709697, connection_type="s3", format="json", connection_options={"path": "s3://andrzul-data-lake/customer/trusted/", "partitionKeys": []}, transformation_ctx="CustomerTrusted_node1726315919322")

job.commit()