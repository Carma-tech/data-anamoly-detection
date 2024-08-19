import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'BUCKET_PREFIX'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the input data path
input_data_path = f"s3://{args['BUCKET_NAME']}/{args['BUCKET_PREFIX']}/data/"

# Read the parquet files from the specified S3 path
df = spark.read.parquet(input_data_path)

# Print the schema of the dataset for verification
df.printSchema()

# Show some rows to verify the data
df.show(5)

# Data Preprocessing Steps

# 1. Handle Missing Values
# Replace NaN in numerical columns with appropriate values (e.g., 0 for numeric columns)
df = df.fillna({
    'passenger_count': 0.0,  # Default to 1 passenger if missing
    'trip_distance': 0.0,  # Default to 0 distance if missing
    'RatecodeID': 1,  # Default to standard rate if missing
    'fare_amount': 0.0,  # Default to 0 fare if missing
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
    'store_and_fwd_flag': 'N',  # Default to "No" for store and forward flag if missing
    'PULocationID': 0,  # Default to 0 (Unknown) if missing
    'DOLocationID': 0,  # Default to 0 (Unknown) if missing
    'payment_type': 1,  # Default to 1 (Credit Card) if missing
})
print('Preprocessed data columns: ', df.columns)

# 2. Handle Invalid Values
# For instance, fare_amount should not be negative
df = df.withColumn('fare_amount', F.when(F.col('fare_amount') < 0, 0.0).otherwise(F.col('fare_amount')))
df = df.withColumn('total_amount', F.when(F.col('total_amount') < 0, 0.0).otherwise(F.col('total_amount')))

# 3. Convert Datatypes if necessary (e.g., Convert dates to TimestampType)
df = df.withColumn('tpep_pickup_datetime', F.col('tpep_pickup_datetime').cast(T.TimestampType()))
df = df.withColumn('tpep_dropoff_datetime', F.col('tpep_dropoff_datetime').cast(T.TimestampType()))

# 4. Add derived columns (if needed)
# Example: Calculate trip duration in minutes
df = df.withColumn('trip_duration', 
                   (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 60)

# 5. Filter out rows with invalid trip duration (e.g., duration should be positive)
df = df.filter(F.col('trip_duration') > 0)

# 6. Optional: Data Aggregation or Summary Statistics
# Example: Calculate the total fare amount and average trip distance
df_summary = df.groupBy().agg(
    F.sum('fare_amount').alias('total_fare_amount'),
    F.avg('trip_distance').alias('avg_trip_distance')
)
print('Summary statistics: ', df_summary.columns)

# Show summary statistics
df_summary.show()

# Write the cleaned and processed data back to S3
output_data_path = f"s3://{args['BUCKET_NAME']}/{args['BUCKET_PREFIX']}/processed_data/"
df.write.mode("overwrite").parquet(output_data_path)
print('Data uploaded to S3')

# Commit the job
job.commit()
