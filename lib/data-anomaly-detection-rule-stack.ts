import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue from 'aws-cdk-lib/aws-glue';
import { BucketDeployment, Source } from 'aws-cdk-lib/aws-s3-deployment';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { join } from 'path';

export class DataAnamolyDetectionStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create S3 bucket
    const anomalyBucket = new s3.Bucket(this, 'AnomalyDetectionBlogBucket', {
      bucketName: 'anamoly-detection-data-bucket',
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

     // Pre-create Glue Table with a minimal schema
    const glueTable = new glue.CfnTable(this, 'AnomalyDetectionGlueTable', {
      catalogId: cdk.Aws.ACCOUNT_ID,
      databaseName: glueDatabase.ref,
      tableInput: {
        name: 'nyctaxi_raw',
        storageDescriptor: {
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
            { name: 'Airport_fee', type: 'double' }, // Add derived columns as well
          ],
          location: `s3://${anomalyBucket.bucketName}/anomaly_detection_blog/processed_data/`,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          compressed: false,
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          },
        },
        tableType: 'EXTERNAL_TABLE',
      },
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
        '--TABLE_NAME': glueTable.ref,
      },
      glueVersion: '4.0',
      workerType: 'G.1X',
      numberOfWorkers: 4,
      timeout: 60,
    });
    

  }
}