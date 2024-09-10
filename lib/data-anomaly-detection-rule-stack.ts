import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue from 'aws-cdk-lib/aws-glue';
import { BucketDeployment, Source } from 'aws-cdk-lib/aws-s3-deployment';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { join } from 'path';

export class DataAnomalyDetectionRuleStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create S3 bucket
    const anomalyBucket = new s3.Bucket(this, 'AnomalyDetectionBlogBucket', {
      // bucketName: 'anomaly-detection-data-bucket',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // Create IAM policy for the S3 bucket
    const anomalyBucketPolicy = new iam.PolicyStatement({
      actions: ['s3:GetObject', 's3:PutObject'],
      resources: [anomalyBucket.bucketArn + '/*'],
    });

    // Deploy scripts to Assets
    new BucketDeployment(this, 'DeployScript', {
      sources: [Source.asset(join(__dirname, '../scripts'))],
      destinationBucket: anomalyBucket,
      destinationKeyPrefix: 'scripts'
    });

    // Deploy dataset to Assets
    new BucketDeployment(this, 'DeployDataset', {
      sources: [Source.asset(join(__dirname, '../data'))],
      destinationBucket: anomalyBucket,
      destinationKeyPrefix: 'anomaly_detection_blog/data'
    });

    // Create IAM role with AWS Glue run permissions and S3 access
    const glueRole = new iam.Role(this, 'AnomalyDetectionGlueServiceRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });

    glueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents',
        's3:PutObject',
        's3:GetObject',
        's3:DeleteObject',
        's3:ListBucket',
        "s3:GetBucketLocation",
        "glue:StartJobRun",
        "glue:CreateJob",
        "glue:DeleteJob",
        "glue:GetJob",
        "glue:GetJobs",
        "glue:UpdateJob",
        'glue:GetTable',
        'glue:CreateTable',
        'glue:UpdateTable',
        "glue:GetDataQualityRuleRecommendationRun",
        "glue:PublishDataQuality",
        "glue:CreateDataQualityRuleset",
        "cloudwatch:PutMetricData"
      ],
      resources: ['*']
    }));

    const gluePolicy = iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole");
    const glueNotebookPolicy = iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceNotebookRole");

    glueRole.addManagedPolicy(gluePolicy);
    glueRole.addManagedPolicy(glueNotebookPolicy);

    // Create Glue Database
    const glueDatabase = new glue.CfnDatabase(this, 'AnomalyDetectionGlueDatabase', {
      catalogId: cdk.Aws.ACCOUNT_ID,
      databaseInput: {
        name: 'anomaly_detection_blog_db',
      },
    });

    // Create Glue Tables based on MYC Taxi datasets
    const datasets = [
      {
        name: 'yellow_tripdata',
        columns: [
          { name: 'VendorID', type: 'int' },
          { name: 'tpep_pickup_datetime', type: 'timestamp' },
          { name: 'tpep_dropoff_datetime', type: 'timestamp' },
          { name: 'passenger_count', type: 'double' },
          { name: 'trip_distance', type: 'double' },
          { name: 'RatecodeID', type: 'int' },
          { name: 'store_and_fwd_flag', type: 'string' },
          { name: 'PULocationID', type: 'int' },
          { name: 'DOLocationID', type: 'int' },
          { name: 'payment_type', type: 'int' },
          { name: 'fare_amount', type: 'double' },
          { name: 'extra', type: 'double' },
          { name: 'mta_tax', type: 'double' },
          { name: 'tip_amount', type: 'double' },
          { name: 'tolls_amount', type: 'double' },
          { name: 'improvement_surcharge', type: 'double' },
          { name: 'total_amount', type: 'double' },
          { name: 'congestion_surcharge', type: 'double' },
          { name: 'Airport_fee', type: 'double' },
        ]
      },
      {
        name: 'green_tripdata',
        columns: [
          { name: 'VendorID', type: 'int' },
          { name: 'lpep_pickup_datetime', type: 'timestamp' },
          { name: 'lpep_dropoff_datetime', type: 'timestamp' },
          { name: 'store_and_fwd_flag', type: 'string' },
          { name: 'RatecodeID', type: 'int' },
          { name: 'PULocationID', type: 'int' },
          { name: 'DOLocationID', type: 'int' },
          { name: 'passenger_count', type: 'double' },
          { name: 'trip_distance', type: 'double' },
          { name: 'fare_amount', type: 'double' },
          { name: 'extra', type: 'double' },
          { name: 'mta_tax', type: 'double' },
          { name: 'tip_amount', type: 'double' },
          { name: 'tolls_amount', type: 'double' },
          { name: 'ehail_fee', type: 'double' },
          { name: 'improvement_surcharge', type: 'double' },
          { name: 'total_amount', type: 'double' },
          { name: 'payment_type', type: 'int' },
          { name: 'trip_type', type: 'int' },
          { name: 'congestion_surcharge', type: 'double' },
        ]
      },
      {
        name: 'fhvh_tripdata',
        columns: [
          { name: 'hvfhs_license_num', type: 'string' },
          { name: 'dispatching_base_num', type: 'string' },
          { name: 'originating_base_num', type: 'string' },
          { name: 'request_datetime', type: 'timestamp' },
          { name: 'on_scene_datetime', type: 'timestamp' },
          { name: 'pickup_datetime', type: 'timestamp' },
          { name: 'dropoff_datetime', type: 'timestamp' },
          { name: 'PULocationID', type: 'int' },
          { name: 'DOLocationID', type: 'int' },
          { name: 'trip_miles', type: 'double' },
          { name: 'trip_time', type: 'int' },
          { name: 'base_passenger_fare', type: 'double' },
          { name: 'tolls', type: 'double' },
          { name: 'bcf', type: 'double' },
          { name: 'sales_tax', type: 'double' },
          { name: 'congestion_surcharge', type: 'double' },
          { name: 'airport_fee', type: 'double' },
          { name: 'tips', type: 'double' },
          { name: 'driver_pay', type: 'double' },
          { name: 'shared_request_flag', type: 'string' },
          { name: 'shared_match_flag', type: 'string' },
          { name: 'access_a_ride_flag', type: 'string' },
          { name: 'wav_request_flag', type: 'string' },
          { name: 'wav_match_flag', type: 'string' },
        ]
      },
      {
        name: 'fhv_tripdata',
        columns: [
          { name: 'dispatching_base_num', type: 'string' },
          { name: 'pickup_datetime', type: 'timestamp' },
          { name: 'dropOff_datetime', type: 'timestamp' },
          { name: 'PUlocationID', type: 'int' },
          { name: 'DOlocationID', type: 'int' },
          { name: 'SR_Flag', type: 'string' },
          { name: 'Affiliated_base_number', type: 'string' },
        ]
      }
    ];

    datasets.forEach((dataset) => {
      new glue.CfnTable(this, `${dataset.name}Table`, {
        catalogId: cdk.Aws.ACCOUNT_ID,
        databaseName: glueDatabase.ref,
        tableInput: {
          name: dataset.name,
          storageDescriptor: {
            columns: dataset.columns,
            location: `s3://${anomalyBucket.bucketName}/anomaly_detection_blog/processed_data/${dataset.name}/`,
            inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            compressed: false,
            serdeInfo: {
              serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            },
          },
          tableType: 'EXTERNAL_TABLE',
          partitionKeys: [
            { name: 'year', type: 'int' },
            { name: 'month', type: 'int' },
            { name: 'day', type: 'int' },
          ],
        },
      });
    });

    const dataGeneratorJobName = 'anomaly_detection_blog_data_generator_job';

    // Create Glue ETL Job
    const glueJob = new glue.CfnJob(this, 'DataGeneratorJob', {
      name: dataGeneratorJobName,
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: `s3://${anomalyBucket.bucketName}/scripts/anomaly_detection_blog_data_generator_job.py`,
        pythonVersion: '3',
      },
      defaultArguments: {
        '--job-bookmark-option': 'job-bookmark-enable',
        '--JOB_NAME': dataGeneratorJobName,
        '--BUCKET_NAME': anomalyBucket.bucketName,
        '--BUCKET_PREFIX': 'anomaly_detection_blog',
        '--DATABASE_NAME': glueDatabase.ref,
        '--TABLE_NAME': '',  // To be specified at runtime depending on the dataset
        '--PREFIX': '',  // To be specified at runtime depending on the dataset
        '--YEAR': '2024',  // To be set dynamically when running the job
        '--MONTH': '5',    // To be set dynamically when running the job
        '--DAY': '1',
      },
      glueVersion: '4.0',
      workerType: 'G.1X',
      numberOfWorkers: 4,
      timeout: 60,
    });


  }
}