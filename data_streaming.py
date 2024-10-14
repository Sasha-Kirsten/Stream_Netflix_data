import sys
import datetime as dt
import base64
import decimal 
import boto3
from pyspark.sql import DataFrame, Row 
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, \
                            ['JOB_NAME',\
                            'aws_region',\
                            'checkpoint_location', \
                            'dynamodb_sink_table'])

sc = SparkContext()
glue = GlueContext(sc)
spark = GlueContext.spark_session
job = Job(GlueContext)
job.init(args['JOB_NAME'], args)

#Read parameters
checkpoint_location = args['checkpoint_locations']
aws_region = args['aws_region']

#DynamoDB config
dynamodb_sink_table = args['dynamodb_sink_table']


