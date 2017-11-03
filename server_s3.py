"""
Client which receives and processes the requests via S3 events and stores messages in DynamoDB.

DynamoDB table schema:
    Partition Key: msg_id (String)
    Range Key: part_number (Number)

You need to set up your bucket to send events to an SQS queue. Which means you also need to create a queue.

You also have to give S3 permission to send messages to your queue. Use the following policy for reference:

{
 "Version": "2008-10-17",
 "Id": "arn:aws:sqs:us-east-1:xxxxxxxxxxxx:queue-name/SQSFromS3Policy",
 "Statement": [
  {
   "Effect": "Allow",
   "Principal": {
     "AWS": "*"  
   },
   "Action": [
    "SQS:SendMessage"
   ],
   "Resource": "arn:aws:sqs:us-east-1:xxxxxxxxxxxx:queue-name",
   "Condition": {
      "ArnLike": {          
      "aws:SourceArn": "arn:aws:s3:*:*:bucket-name"    
    }
   }
  }
 ]
}

"""
from __future__ import print_function

import os
import logging
import json
import urllib2
import time
import boto3

from boto3.dynamodb.conditions import Key

# configure logging
logging.basicConfig(level=logging.INFO)

# environment vars
API_TOKEN = os.getenv("GD_API_TOKEN")
if API_TOKEN is None:
    raise Exception("Must define GD_API_TOKEN environment variable")
API_BASE = os.getenv("GD_API_BASE")
if API_BASE is None:
    raise Exception("Must define GD_API_BASE environment variable")
DYNAMO_TABLE = os.getenv("GD_DYNAMO_TABLE")
if DYNAMO_TABLE is None:
    raise Exception("Must define GD_DYNAMO_TABLE environment variable")
SQS_QUEUE = os.getenv("GD_SQS_QUEUE")
if SQS_QUEUE is None:
    raise Exception("Must define GD_SQS_QUEUE environment variable")

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMO_TABLE)

SQS = boto3.resource('sqs')
QUEUE = SQS.get_queue_by_name(QueueName=SQS_QUEUE)

S3 = boto3.resource('s3')

def server():
    while True:
        logging.info("Getting messages from queue...")
        # get messages from queue
        messages = QUEUE.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=20)
        for message in messages:
            # parse message
            body = json.loads(message.body)
            # process records
            for record in body.get("Records") or []:
                if record["eventName"] != "ObjectCreated:Put":
                    # skip
                    continue
                # read body of object
                s3obj = S3.Object(record['s3']['bucket']['name'], record['s3']['object']['key'])
                body = s3obj.get()["Body"].read()
                # delete object
                s3obj.delete()
                # parse json
                message = json.loads(body)
                # parse message
                msg_id = message['Id']
                part_number = message['PartNumber']
                data = message['Data']
                # put the part received into dynamo
                proceed = store_message(msg_id, part_number, data)
                if proceed is False:
                    # we have already processed this message so don't proceed
                    logging.info("skipping duplicate message")
                    return 'OK'
                # Try to get the parts of the message from the Dynamo.
                check_messages(msg_id)

def store_message(msg_id, part_number, data):
    """
    stores the message locally on a file on disk for persistence
    """
    try:
        table.put_item(
            Item={
                'msg_id': msg_id,
                'part_number': part_number,
                'data': data
            },
            ConditionExpression='attribute_not_exists(msg_id)')
        return True
    except Exception:
        # conditional update failed since we have already processed this message
        # at this point we can bail since we don't want to process again
        # and lose cash moneys
        return False

def check_messages(msg_id):
    """
    checking to see in dynamo if we have the part already
    """
    # do a get item from dynamo to see if item exists
    db_messages = table.query(KeyConditionExpression=Key('msg_id').eq(msg_id))
    # check if both parts exist
    if db_messages["Count"] == 2:
        # app.logger.debug("got a complete message for %s" % msg_id)
        logging.info("Have both parts for msg_id={}".format(msg_id))
        # We can build the final message.
        result = db_messages["Items"][0]["data"] + db_messages["Items"][1]["data"]
        logging.debug("Assembled message: {}".format(result))
        # sending the response to the score calculator
        # format:
        #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
        #   headers -> x-gameday-token = API_token
        #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
        url = API_BASE + '/' + msg_id
        logging.debug("Making request to {} with payload {}".format(url, result))
        req = urllib2.Request(url, data=result, headers={'x-gameday-token':API_TOKEN})
        resp = urllib2.urlopen(req)
        logging.debug("Response from server: {}".format(resp.read()))
        resp.close()

if __name__ == "__main__":
    server()
