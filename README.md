# aws-glue-data-quality-customise-email-notification

This repository consists a Cloudformation template and Lambda function for AWS Glue Data Quality Alert and Notification feature. Here is the architecture diagram

![img.png](img_1.png)

## Getting started


This document is designed to be used with the AWS Big Data Blog titled [Set up alerts and orchestrate data quality rules with AWS Glue Data Quality](https://aws.amazon.com/blogs/big-data/set-up-alerts-and-orchestrate-data-quality-rules-with-aws-glue-data-quality/)

This code uses an input dataset [yellow_tripdata_2022-01](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet/). The CloudFormation template can be launched in any AWS regions and it creates different AWS services example - AWS Step Functions, EventBridge Rule, Lambda Function, Glue Data Quality Ruleset, S3 buckets and SNS topic.

The AWS Cloudformation template deploys different AWS services example - AWS Step Functions, Amazon EventBridge, AWS Lambda Function, AWS Glue Data Quality Rules, Amazon S3 buckets and Amazon SNS topics.

The following sample email provides operational metrics for the AWS Glue Data Quality ruleset evaluation. It provides details about the ruleset name, the number of rules passed or failed, and the score. This helps you visualize the results of each rule along with the evaluation message if a rule fails.

![img.png](img.png)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

