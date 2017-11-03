#!/usr/bin/env python
"""
Client which receives and processes the requests
"""
import os
import logging
import argparse
import urllib2
from flask import Flask, request

import boto3
import json

# environment vars
# API_TOKEN = os.getenv("GD_API_TOKEN")
API_TOKEN = 'c0054dc112'
if API_TOKEN is None:
    raise Exception("Must define GD_API_TOKEN environment variable")
# API_BASE = os.getenv("GD_API_BASE")
API_BASE = 'https://dashboard.gameday.unicornrentals.net/score'
if API_BASE is None:
    raise Exception("Must define GD_API_BASE environment variable")

# defining global vars
MESSAGES = {} # A dictionary that contains message parts

DDB = boto3.resource('dynamodb', region_name='us-west-2')
DDB_TABLE = DDB.Table('onfire-msg')

app = Flask(__name__)

# creating flask route for type argument
@app.route('/', methods=['GET', 'POST'])
def main_handler():
    """
    main routing for requests
    """
    if request.method == 'POST':
        return process_message(request.get_json())
    else:
        return get_message_stats()

def get_message_stats():
    """
    provides a status that players can check
    """
    msg_count = len(MESSAGES.keys())
    return "There are {} messages in the MESSAGES dictionary".format(msg_count)

def sqs_loop():
    # Create SQS client
    sqs = boto3.client('sqs', region_name='us-west-2')
    queue_url = 'https://sqs.us-west-2.amazonaws.com/415062575128/unicorns-sqs'

    while True:
        # Receive message from SQS queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
        )

        if 'Messages' in response:
            logging.info('Got %d messages from SQS', len(response['Messages']))
            # Loop through messages to process them
            for message in response['Messages']:
                receipt_handle = message['ReceiptHandle']
                body = message['Body']
                try:
                    if process_message(json.loads(body)) == 'OK':
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle
                        )
                        logging.info('Deleted message %s', receipt_handle)
                except Exception as e:
                    logging.info("error({0}): {1}".format(e.errno, e.strerror))

def process_message(msg):
    """
    processes the messages by combining parts
    """
    msg_id = msg['Id'] # The unique ID for this message
    part_number = msg['PartNumber'] # Which part of the message it is
    total_parts = msg['TotalParts'] # How many parts we have in total
    data = msg['Data'] # The data of the message

    # log
    logging.info("Processing message for msg_id={} with part_number={} and data={}".format(msg_id, part_number, data))

    # Try to get the parts of the message from the MESSAGES dictionary.
    # If it's not there, create one that has None in both parts
    ## XXX: in mem: parts = MESSAGES.get(msg_id, [None, None])
    parts = [None] * total_parts # Initialise array of length "total_parts"
    response = DDB_TABLE.get_item(Key={'id': msg_id,})
    if 'Item' in response:
        if 'completed' in response['Item']:
            # Message already completed, skip!!
            return 'OK'
        parts = response['Item']['parts']

    # store this part of the message in the correct part of the list
    parts[part_number] = data

    # store the parts in MESSAGES
    ## XXX: in mem: MESSAGES[msg_id] = parts
    DDB_TABLE.put_item(Item={'id': msg_id, 'parts': parts})

    # if both parts are filled, the message is complete
    if None not in parts:
        # app.logger.debug("got a complete message for %s" % msg_id)
        logging.info("Have both parts for msg_id={}".format(msg_id))
        # We can build the final message.
        result = ''.join(parts)
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
        # Mark as sent in Dynamo
        DDB_TABLE.put_item(Item={'id': msg_id, 'parts': parts, 'completed': True})

    return 'OK'

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=['http','sqs'])
    args = parser.parse_args()
    if args.mode == 'http':
        # By default, we disable threading for "debugging" purposes.
        logging.basicConfig(filename='/var/log/unicorn_http.log',level=logging.INFO)
        app.run(host="0.0.0.0", port="80", threaded=True)
    if args.mode == 'sqs':
        logging.basicConfig(filename='/var/log/unicorn_sqs.log',level=logging.INFO)
        sqs_loop()
