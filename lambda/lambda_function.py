import json
import logging
import os

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
ALLOWED_BUCKET = os.environ["ALLOWED_BUCKET"]
ALLOWED_KEY = os.environ["ALLOWED_KEY"]


def lambda_handler(event, context):
    batch_item_failures = []

    logger.info(
        "Received SQS batch with %d record(s)",
        len(event.get("Records", []))
    )

    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")

        try:
            body = json.loads(record["body"])

            bucket = body["bucket"]
            key = body["key"]

            # Enforce least-privilege scope.
            if bucket != ALLOWED_BUCKET:
                raise ValueError(f"Bucket is not allowed: {bucket}")

            if key != ALLOWED_KEY:
                raise ValueError(f"Object key is not allowed: {key}")

            # Get S3 object metadata.
            metadata = s3.head_object(
                Bucket=bucket,
                Key=key
            )

            size = metadata["ContentLength"]
            content_type = metadata.get(
                "ContentType",
                "application/octet-stream"
            )
            last_modified = metadata["LastModified"].isoformat()
            etag = metadata.get("ETag", "").strip('"')
            storage_class = metadata.get("StorageClass", "STANDARD")

            # Build notification message.
            message = (
                "S3 File Notification\n"
                "=====================\n"
                f"File name: {key}\n"
                f"Bucket: {bucket}\n"
                f"Size: {size} bytes\n"
                f"Content type: {content_type}\n"
                f"Last modified: {last_modified}\n"
                f"ETag: {etag}\n"
                f"Storage class: {storage_class}\n"
            )

            # Publish metadata to SNS.
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="S3 File Metadata Notification",
                Message=message
            )

            logger.info(
                "Successfully published SNS notification for %s/%s",
                bucket,
                key
            )

        except (KeyError, json.JSONDecodeError, ValueError) as exc:
            logger.exception(
                "Invalid SQS message %s: %s",
                message_id,
                exc
            )
            batch_item_failures.append(
                {"itemIdentifier": message_id}
            )

        except ClientError as exc:
            logger.exception(
                "AWS API error for SQS message %s: %s",
                message_id,
                exc
            )
            batch_item_failures.append(
                {"itemIdentifier": message_id}
            )

        except Exception as exc:
            logger.exception(
                "Unexpected error for SQS message %s: %s",
                message_id,
                exc
            )
            batch_item_failures.append(
                {"itemIdentifier": message_id}
            )

    return {
        "batchItemFailures": batch_item_failures
    }
