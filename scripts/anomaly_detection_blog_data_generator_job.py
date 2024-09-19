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
                          'JOB_NAME', 'BUCKET_NAME', 'BUCKET_PREFIX',
                          'DATABASE_NAME', 'TABLE_NAME',
                          'PREFIX', 'YEAR', 'MONTH', 'DAY'
                          ])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the input data path
input_data_path = f"s3://{args['BUCKET_NAME']}/{args['BUCKET_PREFIX']}/data/"
prefix = args.get(
    'PREFIX', f'{input_data_path}/yellow_tripdata_2024-05.parquet')

print('prefix: ', prefix)

# Read the parquet files from the specified S3 path
df = spark.read.parquet(prefix)

# Filter by partition
year, month, day = args.get('YEAR'), args.get('MONTH'), args.get('DAY')

print(f'Partition: {year}-{month}-{day}')

# Identify dataset type by column names
columns = df.columns
# Identify dataset type by column names
columns = df.columns
if 'tpep_pickup_datetime' in columns:
    dataset_type = 'yellow'
    pickup_datetime_col = 'tpep_pickup_datetime'
    dropoff_datetime_col = 'tpep_dropoff_datetime'
elif 'lpep_pickup_datetime' in columns:
    dataset_type = 'green'
    pickup_datetime_col = 'lpep_pickup_datetime'
    dropoff_datetime_col = 'lpep_dropoff_datetime'
elif 'hvfhs_license_num' in columns:
    dataset_type = 'fhvh'
    pickup_datetime_col = 'pickup_datetime'
    dropoff_datetime_col = 'dropoff_datetime'
elif 'pickup_datetime' in columns:
    dataset_type = 'fhv'
    pickup_datetime_col = 'pickup_datetime'
    dropoff_datetime_col = 'dropOff_datetime'
else:
    raise ValueError("Unknown dataset type based on columns.")

print(f'Processing {dataset_type} dataset.')

# Data Preprocessing Steps common for all datasets
df = df.withColumn(pickup_datetime_col, F.col(
    pickup_datetime_col).cast(T.TimestampType()))
df = df.withColumn(dropoff_datetime_col, F.col(
    dropoff_datetime_col).cast(T.TimestampType()))

# Add partition keys (year, month, day) for all dataset types
df = df.withColumn('year', F.year(F.col(pickup_datetime_col)))
df = df.withColumn('month', F.month(F.col(pickup_datetime_col)))
df = df.withColumn('day', F.dayofmonth(F.col(pickup_datetime_col)))

# Filter based on the partition predicate provided
if year and month and day:
    partition_filter = (F.col('year') == year) & (
        F.col('month') == month) & (F.col('day') == day)
    df = df.filter(partition_filter)

# Data Preprocessing Steps common for all datasets
if dataset_type in ['yellow', 'green']:
    common_columns = {
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
    }

    if dataset_type == 'yellow':
        common_columns['Airport_fee'] = 0.0

    df = df.fillna(common_columns)

    df = df.fillna({
        'store_and_fwd_flag': 'N',
        'PULocationID': 0,
        'DOLocationID': 0,
        'payment_type': 1,
    })

    df = df.withColumn('fare_amount', F.when(
        F.col('fare_amount') < 0, 0.0).otherwise(F.col('fare_amount')))
    df = df.withColumn('total_amount', F.when(
        F.col('total_amount') < 0, 0.0).otherwise(F.col('total_amount')))
    df = df.filter((F.col('fare_amount') >= 0) &
                   (F.col('fare_amount') <= 500000))

    # Add trip_duration
    pickup_datetime_col = 'tpep_pickup_datetime' if dataset_type == 'yellow' else 'lpep_pickup_datetime'
    dropoff_datetime_col = 'tpep_dropoff_datetime' if dataset_type == 'yellow' else 'lpep_dropoff_datetime'

    df = df.withColumn('year', F.year(F.col(pickup_datetime_col)))
    df = df.withColumn('month', F.month(F.col(pickup_datetime_col)))
    df = df.withColumn('day', F.dayofmonth(F.col(pickup_datetime_col)))

    df = df.withColumn('trip_duration',
                       (F.unix_timestamp(dropoff_datetime_col) -
                        F.unix_timestamp(pickup_datetime_col)) / 60)

    df = df.filter(F.col('trip_duration') > 0)

elif dataset_type == 'fhvh':
    df = df.withColumn('pickup_datetime', F.col(
        'pickup_datetime').cast(T.TimestampType()))
    df = df.withColumn('dropoff_datetime', F.col(
        'dropoff_datetime').cast(T.TimestampType()))

    df = df.withColumn('year', F.year(F.col('pickup_datetime')))
    df = df.withColumn('month', F.month(F.col('pickup_datetime')))
    df = df.withColumn('day', F.dayofmonth(F.col('pickup_datetime')))

elif dataset_type == 'fhv':
    df = df.withColumn('pickup_datetime', F.col(
        'pickup_datetime').cast(T.TimestampType()))
    df = df.withColumn('dropOff_datetime', F.col(
        'dropOff_datetime').cast(T.TimestampType()))

    # Add partition columns
    df = df.withColumn('year', F.year(F.col('pickup_datetime')))
    df = df.withColumn('month', F.month(F.col('pickup_datetime')))
    df = df.withColumn('day', F.dayofmonth(F.col('pickup_datetime')))

# Write the cleaned and processed data back to S3
output_data_path = f"s3://{args['BUCKET_NAME']}/{args['BUCKET_PREFIX']}/processed_data/{dataset_type}/"
df.write.mode("overwrite").parquet(output_data_path)
print('Data uploaded to S3')

# Convert Spark DataFrame to Glue DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Construct the DQDL ruleset with a generic rule (customize for each dataset type if needed)
ruleset = f"""
     Rules = [
        ColumnValues "fare_amount" between 0 and 400000,
        RowCount > 100,
        ColumnCount > 5,
        IsComplete "VendorID"
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
        "enableUpdateCatalog": True,  # Ensures the table is created or updated
        "partitionKeys": ["year", "month", "day"],
    }
)
print('Job completed')
# Commit the job
job.commit()
