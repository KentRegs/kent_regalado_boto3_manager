from botocore.exceptions import ClientError
import argparse
import logging
import boto3

log = logging.getLogger(__name__)

def create_sns_topic(topic_name):
	sns = boto3.client('sns')
	sns.create_topic(Name=topic_name)

	return True

def list_sns_topics(next_token=None):
	sns = boto3.client('sns')
	params = {'NextToken': next_token} if next_token else {}
	topics = sns.list_topics(**params)
	
	print(topics.get('Topics', []), topics.get('NextToken', None))
	return topics.get('Topics', []), topics.get('NextToken', None)

def list_sns_subscriptions(next_token=None):
	sns = boto3.client('sns')
	params = {--'NextToken': next_token} if next_token else {}
	subscriptions = sns.list_subscriptions(**params)
	
	print(subscriptions.get('Subscriptions', []))
	return subscriptions.get('Subscriptions', []),
	subscriptions.get('NextToken', None)


def subscribe_sns_topic(topic_arn, mobile_number):
	sns = boto3.client('sns')
	params = {
		'TopicArn': topic_arn,
		'Protocol': 'sms',
		'Endpoint': mobile_number,
	}
	res = sns.subscribe(**params)
	print(res)
	
	return True

def send_sns_message(topic_arn, message):
	sns = boto3.client('sns')
	params = {
		'TopicArn': topic_arn,
		'Message': message,
	}
	res = sns.publish(**params)
	print(res)

	return True

def unsubscribe_sns_topic(subscription_arn):
	sns = boto3.client('sns')
	params = {
		'SubscriptionArn': subscription_arn,
	}
	res = sns.unsubscribe(**params)
	print(res)

	return True

def delete_sns_topic(topic_arn):
	# This will delete the topic and all it's subscriptions.
	sns = boto3.client('sns')
	sns.delete_topic(TopicArn=topic_arn)

	return True

if __name__ == '__main__':	
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()

	sns_create = subparsers.add_parser('create')
	sns_create.add_argument('topicname', help='SNS topic name.')
	sns_create.set_defaults(func=create_sns_topic)

	sns_list_topics = subparsers.add_parser('list_topics')
	sns_list_topics.add_argument('--nexttoken', help='Next token.')
	sns_list_topics.set_defaults(func=list_sns_topics)	

	sns_list_subs = subparsers.add_parser('list_subs')
	sns_list_subs.add_argument('--nexttoken', help='Next token.')
	sns_list_subs.set_defaults(func=list_sns_subscriptions)	

	sns_subscribe = subparsers.add_parser('sub')
	sns_subscribe.add_argument('topicarn', help='Topic Amazon Resource Name.')
	sns_subscribe.add_argument('mobilenum', help='Mobile number.')
	sns_subscribe.set_defaults(func=subscribe_sns_topic)	

	sns_send = subparsers.add_parser('send')
	sns_send.add_argument('topicarn', help='Topic Amazon Resource Name.')
	sns_send.add_argument('msg', help='Message.')
	sns_send.set_defaults(func=send_sns_message)

	sns_unsubscribe = subparsers.add_parser('unsub')
	sns_unsubscribe.add_argument('subarn', help='Subscription Amazon Resource Name.')
	sns_unsubscribe.set_defaults(func=unsubscribe_sns_topic)

	sns_delete = subparsers.add_parser('delete')
	sns_delete.add_argument('topicarn', help='Topic Amazon Resource Name.')
	sns_delete.set_defaults(func=delete_sns_topic)

	args = parser.parse_args()

	if hasattr(args, 'func'):
		if args.func.__name__ == 'create_sns_topic':
			args.func(topic_name=args.topicname)
		elif args.func.__name__ == 'list_sns_topics':
			args.func(next_token=args.nexttoken)
		elif args.func.__name__ == 'list_sns_subscriptions':
			args.func(next_token=args.nexttoken)
		elif args.func.__name__ == 'subscribe_sns_topic':
			args.func(topic_arn=args.topicarn, mobile_number=args.mobilenum)
		elif args.func.__name__ == 'send_sns_message':
			args.func(topic_arn=args.topicarn, message=args.msg)
		elif args.func.__name__ == 'unsubscribe_sns_topic':
			args.func(subscription_arn=args.subarn)
		elif args.func.__name__ == 'delete_sns_topic':
			args.func(topic_arn=args.topicarn)