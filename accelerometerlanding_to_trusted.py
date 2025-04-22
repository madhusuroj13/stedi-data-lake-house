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
AccelerometerTrusted_node1693745022656=glueContext.create_dynamic_frame.from_options()
format_options={"multiline":False},
connection_type='s3',
  format='json',
connection_options={"paths":["s3://stedi-s3/customer/curated/"].'recurse':True},
transformation_ctx="CustomerCurated_node1693745022656",
)

customerTrusted_node1693315552442=glueContext.create_dynamic_frame.from_options(
    format_options={"multiline":False},
    connection_types='s3',
      format='json',
    connection_options={
        "paths":[
            's3://stedi-s3/step_trainer/landing/"'],
            "recurse":True,
    },
    transformation_ctx="StepTrainerLanding_node1",
    )
    CustomerCurated_node1693745022656=glueContext.create_dynamic_frame.from_options(
format_options={"multiline":False},
connection_type='s3',
  format='json',
connection_options={"paths":["s3://stedi-s3/customer/curated/"].'recurse':True},
transformation_ctx="CustomerCurated_node1693745022656",
)

CustomerPrivacyFilter_nodeglueContext.create_dynamic_frame.from_options(
    format_options={"multiline":False},
    connection_types='s3',
      format='json',
    connection_options={
        "paths":[
            's3://stedi-s3/step_trainer/landing/"'],
            "recurse":True,
    },
    transformation_ctx="StepTrainerLanding_node1",
    )
    
    DropFields_node1693745286659=ApplyMapping.apply(
        frame=join_node1693745105307,
        mappings=[
            ("sensorReadingTime","long","sensorReadingTime","long"),
            ("serialnumber","string","serialnumber","string"),
            ("distanceFromobject","int","distanceFromObject","int")
            ],
            transformation_ctx="DropFields_node1693745286659",
            )
AccelerometerTrusted_node3=glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node3=glueContext.write_dynamic_frame.from_options(
connection_type="s3",
  format="json",
connection_options={
    "path":"s3://stedi-s3/step_trainer/trusted/",
    "compression":"snappy",
    "partitionkeys":[],
},
transformation_ctx="StepTrainerTrusted_node3",
)
job.commit()
   
