#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { DataAnamolyDetectionStack } from '../lib/data-anomaly-detection-rule-stack';

const app = new cdk.App();
new DataAnamolyDetectionStack(app, 'DataAnamolyDetectionStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  }
});

app.synth();