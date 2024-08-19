import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
                          'JOB_NAME', 'BUCKET_NAME', 'BUCKET_PREFIX', 'DATABASE_NAME', 'TABLE_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the input data path
input_data_path = f"s3://{args['BUCKET_NAME']}/{args['BUCKET_PREFIX']}/data/"
prefix = f'{input_data_path}/yellow_tripdata_2024-05.parquet'
print('prefix: ', prefix)
# Read the parquet files from the specified S3 path
df = spark.read.parquet(prefix)

# Print the schema of the dataset for verification
df.printSchema()

# Show some rows to verify the data
df.show(5)
print('Columns: ', df.columns)

# Data Preprocessing Steps

# 1. Handle Missing Values
# Replace NaN in numerical columns with appropriate values (e.g., 0 for numeric columns)
df = df.fillna({
    'passenger_count': 0.0,
    'trip_distance': 0.0,
    'RatecodeID': 1,
    'fare_amount': 0.0,
    'extra': 0.0,
    'mta_tax': 0.0,
    'tip_amount': 0.0,
    'tolls_amount': 0.0,
    'improvement_surcharge': 0.0,
    'total_amount': 0.0,
    'congestion_surcharge': 0.0,
    'Airport_fee': 0.0,
})

# Replace None in categorical columns with a default value (e.g., "unknown")
df = df.fillna({
    'store_and_fwd_flag': 'N',
    'PULocationID': 0,
    'DOLocationID': 0,
    'payment_type': 1,
})
print('Preprocessed data columns: ', df.columns)

# 2. Handle Invalid Values
df = df.withColumn('fare_amount', F.when(F.col('fare_amount')
                   < 0, 0.0).otherwise(F.col('fare_amount')))
df = df.withColumn('total_amount', F.when(
    F.col('total_amount') < 0, 0.0).otherwise(F.col('total_amount')))

# Filter out unrealistic fare_amout values
df = df.filter((F.col('fare_amount') >= 0) & (F.col('fare_amount') <= 500000))

# 3. Convert Datatypes if necessary (e.g., Convert dates to TimestampType)
df = df.withColumn('tpep_pickup_datetime', F.col(
    'tpep_pickup_datetime').cast(T.TimestampType()))
df = df.withColumn('tpep_dropoff_datetime', F.col(
    'tpep_dropoff_datetime').cast(T.TimestampType()))

# 4. Add derived columns (if needed)
df = df.withColumn('trip_duration',
                   (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 60)

# 5. Filter out rows with invalid trip duration (e.g., duration should be positive)
df = df.filter(F.col('trip_duration') > 0)

# Write the cleaned and processed data back to S3
output_data_path = f"s3://{args['BUCKET_NAME']}/{args['BUCKET_PREFIX']}/processed_data/"
df.write.mode("overwrite").parquet(output_data_path)
print('Data uploaded to S3')

# Convert Spark DataFrame to Glue DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Construct the DQDL ruleset
ruleset = """
    Rules = [
        ColumnValues "fare_amount" between 0 and 400000,
        RowCount > avg(5),
        ColumnCount > 5,
        IsComplete "VendorID",
        CustomSQL "select count(*) from __THIS__ where passenger_count = 0",
    ],
    Analyzers = [
        DistinctValuesCount "PULocationID",
        RowCount > 1000
    ]
    """

# Evaluate data quality
dq_results = EvaluateDataQuality.apply(
    frame=dynamic_frame,
    ruleset=ruleset,
    # analyzers=analyzers,
    publishing_options={
        "dataQualityEvaluationContext": 'default_context' 
    }
)
print('Evaluation results: ', dq_results)
dq_results.printSchema()
dq_results.toDF().show()
# Store the data in Glue Data Catalog dynamically
glueContext.write_dynamic_frame.from_catalog(
    frame=dynamic_frame,
    database=args['DATABASE_NAME'],
    table_name=args['TABLE_NAME'],
    additional_options={
        # "partitionKeys": ["year", "month", "day", "hour"],
        "enableUpdateCatalog": True  # Ensures the table is created or updated
    }
)
print('Job completed')
# Commit the job
job.commit()
