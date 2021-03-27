from datetime import datetime as dt, timedelta as td
from boto3.dynamodb.conditions import Key, Attr
import requests
import datetime
import logging
import pprint
import boto3
import json
import re

log = logging.getLogger(__name__) 
log.setLevel(logging.INFO)

def create_sns_topic(topic_name):
	sns = boto3.client('sns')
	sns.create_topic(Name=topic_name)

	return True
	
def subscribe_sns_topic(topic_arn, email_addr):
	sns = boto3.client('sns')
	params = {
		'TopicArn': topic_arn,
		'Protocol': 'email',
		'Endpoint': email_addr,
	}
	res = sns.subscribe(**params)
	
	return True

def get_topic_arns():
	sns = boto3.client('sns')
	topics = sns.list_topics()
	pattern = re.compile('kent')
  
	arns = [arn for arn in topics['Topics'] if pattern.search(arn['TopicArn'])]

	return arns
  
def publish_sns_message(log_level, topic_arns, msg):
	sns = boto3.client('sns')
	pattern = re.compile(log_level)
	topic_arn = [arn for arn in topic_arns if pattern.search(arn['TopicArn'])]

	if log_level != 'CRITICAL':
		params = {
			'TopicArn': topic_arn[0]['TopicArn'],
			'Message': msg
		}
		
		response = sns.publish(**params)
		log.info(params)
		log.info(response)
	else:
		response = requests.post(
			url = 'https://pmh3i3xv2e.execute-api.ap-southeast-1.amazonaws.com/development/ac2e_email_sender_service',
			json = {
				'to': 'matthewkent.regalado@mynt.xyz',
				'subject': '[CRITICAL ERROR]',
				'body': 'An error has occurred in the application.'
			}
		)
		
		log.info(response.json())
		
def create_dynamo_table(table_name, pk, pkdef):
	ddb = boto3.resource('dynamodb')
	table = ddb.create_table(
		TableName=table_name,
		KeySchema=pk,
		AttributeDefinitions=pkdef,
		ProvisionedThroughput={
			'ReadCapacityUnits': 5,
			'WriteCapacityUnits': 5,
		}
	)	

	table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

	return table

def get_dynamo_table(table_name):
	ddb = boto3.resource('dynamodb')

	return ddb.Table(table_name)

def dynamodb_logger(source_application, timestamp, column_dict):
	table = get_dynamo_table('kent_capstone_logs')
	keys = {
		'source_application': source_application,
		'timestamp': timestamp,
	}
	
	column_dict.update(keys)
	table.put_item(Item=column_dict)

	return table.get_item(Key=keys)['Item']
	
def query_timestamp(key_expr, filter_expr=None):
	# Query requires that you provide the key filters
	table = get_dynamo_table('kent_capstone_logs')
	params = {
		'KeyConditionExpression': key_expr,
	}
	if filter_expr:
		params['FilterExpression'] = filter_expr
	
	res = table.query(**params)
	ts_list = []

	for row in res['Items']:
		ts_list.append(row['timestamp'])

	# return latest timestamp of matched key expression and/or filter expression
	if len(ts_list) > 0:
		return ts_list[-1]
	else:
		return 1
	
def throttling_mechanism(source_application, log_level):
	hash_key = Key('source_application').eq(source_application)
	range_key = Attr('log_level').eq(log_level)
	
	latest_ts = query_timestamp(hash_key, range_key)
	# print(type(latest_ts))
	# print(latest_ts)
	
	if latest_ts != 1:
		date_time_obj = dt.strptime(latest_ts, '%Y-%m-%d %H:%M:%S')
		
		# add 5 minutes to the latest timestamp object
		time_diff = td(minutes=5)
		checker = date_time_obj + time_diff
		
		curr_ts = dt.utcnow()
		
		# check if current timestamp is 5 minutes after the matched latest timestamp in the dynamodb table.
		if curr_ts > checker:
			return True
		else:
			return False
	else:
		return True
  
def lambda_handler(event, context):
	# # Create SNS topics for each log level
	# create_sns_topic(topic_name)
	# # subscribe email to SNS topics
	# subscribe_sns_topic(topic_arn, email_addr)
	
	# get the ARNs of all the SNS topics that will be used
	arns = get_topic_arns()
	
	# extract contents of the event parameter
	# i.e. convert json object into a dictionary
	body = json.loads(event['body'])
	# convert json object to string
	json_str_body = event['body']
	
	# instantiate parameters
	msg = body['message']
	log_level = body['log_level']
	src_app = body['source_application']
	
	log.info(f'arns: {arns}')
	log.info(f'event body: {json_str_body}')
	
	# publish an SNS message if an identical message hasn't been sent within the last 5 minutes
	if throttling_mechanism(src_app, log_level):
		publish_sns_message(log_level, arns, msg)
	
	# push logs to dynamodb
	formatted_ts = str(datetime.datetime.now()).split('.')[0]
	dynamodb_logger(src_app, formatted_ts, body)
	
	return {         
		'statusCode': 200,    
		'body': json.dumps(f'Successfully processed source application <{src_app}>!')
	}