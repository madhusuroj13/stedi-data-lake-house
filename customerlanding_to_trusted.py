import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

CustomerLanding_node1=glueContext.create_dynamic_frame.from_options(
format_options={"multiline":False},
connection_type='s3',
  format='json',
connection_options={"paths":["s3://stedi-s3/customer/curated/"].'recurse':True},
transformation_ctx="CustomerCurated_node1693745022656",
)

privacyFilter_node1693740592249=Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row:(not(row["ShareWithReasearchAsofDate"]==0)))
    transformation_ctx="privacyFilter_node1693740592249",
    )
CustomerTrusted_node3=glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node3=glueContext.write_dynamic_frame.from_options(
connection_type="s3",
  format="json",
connection_options={
    "path":"s3://stedi-s3/step_trainer/trusted/",
},
transformation_ctx="StepTrainerTrusted_node3",
)
job.commit()
   
