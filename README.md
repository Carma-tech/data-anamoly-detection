# Welcome to Anomaly Data Quality Detection Rule

## Project Description

This project is designed to build an automated data pipeline for detecting anomalies in various `NYC Taxi` datasets. The pipeline is built using AWS services such as AWS Glue for ETL (Extract, Transform, Load) operations and AWS CDK (Cloud Development Kit) for infrastructure as code (IaC). The pipeline handles four different types of datasets `yello_tripdata`, `green_tripdata`, `fvh_tripdata`, and `fhvh_tripdata`, each with its own schema and data processing requirements. The AWS Glue job reads the input data, processes it according to the specified dataset, runs data quality checks, and then stores the processed data in S3. The job also dynamically updates AWS Glue Data Catalog tables, making the processed data available for further analysis.

## Project Structure
```text
├── README.md
├── cdk
│   ├── bin
│   │   └── app.ts
│   ├── lib
│   │   └── data-anomaly-detection-stack.ts
│   ├── scripts
│   │   └── anomaly_detection_blog_data_generator_job.py
│   ├── data
│   │   ├── yellow_tripdata_2024-05.parquet
│   │   ├── green_tripdata_2024-05.parquet
│   │   ├── fhv_tripdata_2024-05.parquet
│   │   └── fhvh_tripdata_2024-05.parquet
└── cdk.json
```

+ __cdk/lib/data-anomaly-detection-stack.ts:__ Contains the CDK code for creating AWS infrastructure, including S3 buckets, Glue jobs, Glue tables, and IAM roles.
+ __cdk/scripts/anomaly_detection_blog_data_generator_job.py:__ The Python script executed by AWS Glue to process datasets, apply data quality checks, and store the data.
+ __cdk/data:__ Contains sample datasets for `yellow`, `green`, `fhv`, and `fhvh tripdata`.

###### Note: The `fhvh_tripdata` is not available in `data` folder because that's a large amount of dataset and the size is 500 MB, so this will take some time when cloning repo. You can download that dataset from the following link.

[https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)


## Prerequisites

* __Node.js__ and npm installed on your machine.
* __AWS CLI__ configured with appropriate credentials.
* __AWS CDK__ installed globally via npm:
```bash
npm install -g aws-cdk
```

## Instructions

### 1: Clone the Repository and Install Dependencies

```bash
git clone https://github.com/Carma-tech/data-anamoly-detection
cd data-anamoly-detection
npm install
```

### 2: Build and Deploy the CDK Stack

```bash
cdk bootstrap
cdk synth
cdk deploy DataAnomalyDetectionRuleStack
```

This will provision the required AWS infrastructure in your AWS account.

The CDK deployment will automatically upload the datasets and the Glue job scripts to the S3 bucket created during the deployment.

### 3: Run the Glue Job for a Specific Dataset

You can just start the Glue job for the default dataset:

```bash
aws glue start-job-run --job-name anomaly_detection_blog_data_generator_job
```

**OR**

You can start the Glue job for a specific dataset by passing the appropriate `PREFIX` and `TABLE_NAME` arguments. For example, to run the job for the `green_tripdata` dataset:

```bash
aws glue start-job-run \
  --job-name anomaly_detection_blog_data_generator_job \
  --arguments '{"--PREFIX":"s3://anomaly-detection-data-bucket/anomaly_detection_blog/data/green_tripdata_2024-05.parquet","--TABLE_NAME":"yellow_tripdata","---YEAR":"2024","--MONTH":"5","--DAY":"1"}'
```

This command starts the Glue job, which processes the green_tripdata dataset, applies data quality checks, and stores the processed data in S3.

#### 3.1: Generalizing for Other Datasets

You can replace the `--PREFIX` and `--TABLE_NAME` values to match the dataset you want to process:

* For `yellow_tripdata`:
```bash
aws glue start-job-run \
  --job-name anomaly_detection_blog_data_generator_job \
  --arguments '{"--PREFIX":"s3://anomaly-detection-data-bucket/anomaly_detection_blog/data/yellow_tripdata_2024-05.parquet","--TABLE_NAME":"yellow_tripdata","---YEAR":"2024","--MONTH":"5","--DAY":"1"}'
```

* For `fhvh_tripdata`
```bash
aws glue start-job-run \
  --job-name anomaly_detection_blog_data_generator_job \
  --arguments '{"--PREFIX":"s3://anomaly-detection-data-bucket/anomaly_detection_blog/data/fhvh_tripdata_2024-05.parquet","--TABLE_NAME":"yellow_tripdata","---YEAR":"2024","--MONTH":"5","--DAY":"1"}'
```

* For `fhv_tripdata`
```bash
aws glue start-job-run \
  --job-name anomaly_detection_blog_data_generator_job \
  --arguments '{"--PREFIX":"s3://anomaly-detection-data-bucket/anomaly_detection_blog/data/fvh_tripdata_2024-05.parquet","--TABLE_NAME":"yellow_tripdata","---YEAR":"2024","--MONTH":"5","--DAY":"1"}'
```


### 4: Monitor the Glue Job

You can monitor the progress of the Glue job via the AWS Glue Console. Once the job completes successfully, the processed data will be available in the specified S3 location, and the Glue Data Catalog will be updated with the new dataset.

To test or check the data quality DQ run data results in AWS console: `Glue > ETL jobs > Visual ETL > anomaly_detection_blog_data_generator_job > Data Quality`

### 5: Analyze Processed Data

After the Glue job completes, you can query the processed data using AWS Athena, Redshift Spectrum, or any other compatible tool that supports Glue Data Catalog as a data source.

### 6: Clean Up Resources

To avoid incurring unnecessary charges, you can delete the AWS resources created by the CDK stack:

```bash
cdk destroy DataAnomalyDetectionRuleStack
```
#### Read more about

[https://aws.amazon.com/blogs/big-data/introducing-aws-glue-data-quality-anomaly-detection/](https://aws.amazon.com/blogs/big-data/introducing-aws-glue-data-quality-anomaly-detection/)
