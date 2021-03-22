from botocore.exceptions import ClientError
import argparse
import logging
import boto3

log = logging.getLogger(__name__)

def list_log_groups(group_name=None, region_name=None):
	cwlogs = boto3.client('logs', region_name=region_name)
	params = {
		'logGroupNamePrefix': group_name,
	} if group_name else {}
	res = cwlogs.describe_log_groups(**params)

	print(res['logGroups'])
	return res['logGroups']

def list_log_group_streams(group_name, stream_name=None, region_name=None):
	cwlogs = boto3.client('logs', region_name=region_name)
	params = {
		'logGroupName': group_name,
	} if group_name else {}
	if stream_name:
		params['logStreamNamePrefix'] = stream_name
		res = cwlogs.describe_log_streams(**params)

	return res['logStreams']

def filter_log_events(
	group_name, filter_pat,
	start=None, stop=None,
	region_name=None
):
	cwlogs = boto3.client('logs', region_name=region_name)
	params = {
		'logGroupName': group_name,
		'filterPattern': filter_pat,
	}
	if start:
		params['startTime'] = start
		if stop:
			params['endTime'] = stop
		res = cwlogs.filter_log_events(**params)
		
		return res['events']

if __name__ == '__main__':	
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()

	cw_list_grps = subparsers.add_parser('list_grps')
	cw_list_grps.add_argument('--groupname', help='Group name.')
	cw_list_grps.add_argument('--region', help='AWS Region.')
	cw_list_grps.set_defaults(func=list_log_groups)

	cw_list_grp_strms = subparsers.add_parser('list_grp_strms')
	cw_list_grp_strms.add_argument('groupname', help='Group name.')
	cw_list_grp_strms.add_argument('--streamname', help='Stream name.')
	cw_list_grp_strms.add_argument('--region', help='AWS Region.')
	cw_list_grp_strms.set_defaults(func=list_log_group_streams)

	cw_filter_log_events = subparsers.add_parser('filter')
	cw_filter_log_events.add_argument('groupname', help='Group name.')
	cw_filter_log_events.add_argument('filterpat', help='Stream name.')
	cw_filter_log_events.add_argument('--start', help='Start.')
	cw_filter_log_events.add_argument('--stop', help='Stop.')
	cw_filter_log_events.add_argument('--region', help='AWS Region.')
	cw_filter_log_events.set_defaults(func=filter_log_events)

	args = parser.parse_args()

	if hasattr(args, 'func'):
		if args.func.__name__ == 'list_log_groups':
			args.func(group_name=args.groupname, region_name=args.region)
		elif args.func.__name__ == 'list_log_group_streams':
			args.func(\
				group_name=args.groupname, 
				stream_name=args.streamname,
				region_name=args.region
			)
		elif args.func.__name__ == 'filter_log_events':
			args.func(
				group_name=args.groupname, 
				filter_pat=args.filterpat,
				start=args.start,
				stop=args.stop,
				region_name=region
			)