# AWS CLI Event-Driven File Notification Pipeline

A serverless AWS pipeline that processes an S3 file notification through SQS, Lambda, and SNS, then sends the file metadata to an email subscriber.

## Architecture

S3 Upload -> SQS -> Lambda -> S3 HeadObject -> SNS -> Email
