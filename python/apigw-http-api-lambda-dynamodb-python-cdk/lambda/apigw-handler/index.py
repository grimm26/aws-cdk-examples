# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    
    # Extract security context from request
    request_context = event.get("requestContext", {})
    request_id = request_context.get("requestId", "unknown")
    source_ip = request_context.get("identity", {}).get("sourceIp", "unknown")
    user_agent = request_context.get("identity", {}).get("userAgent", "unknown")
    
    # Structured logging with security context
    log_entry = {
        "request_id": request_id,
        "source_ip": source_ip,
        "user_agent": user_agent,
        "table_name": table,
    }
    
    logger.info(json.dumps({**log_entry, "message": "Request received"}))
    
    try:
        if event.get("body"):
            item = json.loads(event["body"])
            logger.info(json.dumps({**log_entry, "message": "Processing payload", "item_id": item.get("id")}))
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            logger.info(json.dumps({**log_entry, "message": "Successfully inserted data", "item_id": id}))
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            logger.info(json.dumps({**log_entry, "message": "No payload provided, using default"}))
            default_id = str(uuid.uuid4())
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": default_id},
                },
            )
            logger.info(json.dumps({**log_entry, "message": "Successfully inserted default data", "item_id": default_id}))
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        logger.error(json.dumps({**log_entry, "message": "Error processing request", "error": str(e)}))
        raise
