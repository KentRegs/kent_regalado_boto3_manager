from pathlib import Path
import requests
import argparse
import logging
import shutil
import json
import sys
import re
import os

import pprint

# Initialize logger for this module.
LOG_FMT_STRING = (
	'[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s'
)
logging.basicConfig(level=logging.INFO, format=LOG_FMT_STRING)
log = logging.getLogger(__name__)

# create a 'copied_files' folder
def checkDir(name, list_path):
	file_path = ''.join((list_path, name))

	if os.path.isdir(file_path):
		log.info(f'<{name}> directory exists')
	else:
		log.info(f'Does not exist. Creating <{name}> directory...')
		# subprocess.run(['mkdir', name])
		os.mkdir(file_path)

def rename_file(file_path, new_name, counter=None):
	new_name = f'{new_name}{counter}{file_path.suffix}'

	return new_name

def get_files(target_path, filter_pat=None):
	try:
		if filter_pat:
			filter_pat = re.compile(filter_pat)
			log.debug(
				f'Searching files matching pattern: {filter_pat.pattern}'
			)
	except Exception as err:
		msg = f'Invalid filter: {err}'
		log.error(msg)
		raise Exception(msg)

	for file_ in target_path.iterdir():
		if filter_pat:
			if filter_pat.match(file_.name):
				yield file_
			else:
				log.debug(f'File {file_.name} did not match pattern.')
		else:
			yield file_

def copy_file(target_dir, file_pattern=None, log_level=None):
	source_application = os.path.basename(__file__)
	orig_dir = target_dir
	target_dir = Path(target_dir)

	if not target_dir.is_dir():
		log_level = 'error'
		message = '[target_dir] does not exist or is not a directory.'
		log.error(message)
		
		temp = {
			"log_level": log_level.upper(),
			"message": message,
			"details": "",
			"source_application": source_application
		}

		payload = json.dumps(temp)
		callLoggingAndNotifAPI(payload)	
	else:
		base_path = '/home/kent/Desktop/python/python_capstone/python_capstone_project/'
		dir_name = 'copied_files'

		# check if 'copied_files' directory exists
		# if not, create one
		checkDir(dir_name, base_path)

		dir_path = ''.join((base_path, dir_name))
		request_message = ''
		message_list = []

		# iterate through matched files 
		for file_ in get_files(target_dir, filter_pat=file_pattern):
			log.info(f'Found {file_}')
		
			# create and store copy of each file in the 'copied_files' directory
			src = orig_dir + file_.name
			shutil.copy(src, dir_path)
			message = f'Copied {src} -> {dir_path},'
			log.info(f'{message}\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

			# populate message_list
			message_list.append(message)

		# append each row of 'message_list' into 'request_message',
		# which will be passed as the 'key2' or 'Message' parameter
		# for the 'callLoggingAndNotifAPI' function
		for row in message_list:
			messages = row.strip()
			# add a newline per row for better readability
			request_message = '\n'.join((request_message, messages))
			
		if len(request_message) == 0:
			request_message = f'File/s with file pattern <{file_pattern}> not found.'
			log_level = 'error'

		# generate payload to be passed as an argument to the 
		# 'callLoggingAndNotifAPI' function
		temp = {
			"log_level": log_level.upper(),
			"message": request_message,
			"details": "",
			"source_application": source_application
		}

		payload = json.dumps(temp)
		callLoggingAndNotifAPI(payload)	
			
		return True

def bulk_rename_files(target_dir, new_name, file_pattern=None, log_level=None):
	source_application = os.path.basename(__file__)
	target_dir = Path(target_dir)		

	if not target_dir.is_dir():
		message = f'<{target_dir}> does not exist or is not a directory.'
		log.error(message)

		# generate payload to be passed as an argument to the 
		# callLoggingAndNotifAPI function
		temp = {
			"log_level": 'ERROR',
			"message": message,
			"details": "",
			"source_application": source_application
		}

		payload = json.dumps(temp)
		callLoggingAndNotifAPI(payload)

		return False
	else:
		request_message = ''
		message_list = []
		counter = 1

		# iterate through matched files 
		for file_ in get_files(target_dir, filter_pat=file_pattern):
			log.info(f'Found {file_}')

			# generate new filenames and paths for every matched file 
			updated_name = rename_file(file_, new_name, counter=counter)
			new_path = file_.parent.joinpath(updated_name)		

			# update paths
			shutil.move(file_, new_path)
			message = f'Renamed {file_.name} -> {new_path.name}, '
			log.info(f'{message}\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')

			counter += 1

			# populate message_list
			message_list.append(message)

		# append each row of message_list into request_message,
		# which will be passed as the 'key2' or 'Message' parameter
		# for the 'callLoggingAndNotifAPI' function
		for row in message_list:
			messages = row.strip()
			# add a newline per row for better readability
			request_message = '\n'.join((request_message, messages))
		
		if len(request_message) == 0:
			request_message = f'File/s with file pattern <{file_pattern}> not found.'
			log_level = 'error'

		# generate payload to be passed as an argument to the 
		# 'callLoggingAndNotifAPI' function
		temp = {
			"log_level": log_level.upper(),
			"message": request_message,
			"details": "",
			"source_application": source_application
		}

		payload = json.dumps(temp)
		callLoggingAndNotifAPI(payload)
			
		return True

def callLoggingAndNotifAPI(payload):
	log.info('Processing request...')

	response = requests.post(
		url="https://qm5etw0909.execute-api.ap-southeast-1.amazonaws.com/default/kent-capstone",
		headers={'x-api-key': 'LKTJdj3oNC6qSjKHeXQb1azDUcSxN1cq1VvrPPls'},
		data=payload
	)

	log.info(f'{response.json()}')

def main(args):
	try:		
		
		if args.copy == 'True':
			success = copy_file(
				args.target_dir, 
				args.file_pattern, 
				args.log_level
			)
		else:			
			success = bulk_rename_files(
				args.target_dir, 
				args.new_name, 
				args.file_pattern,
				args.log_level
			)
		
		if success:
			sys.exit(0)
		else:
			sys.exit(1)
	except Exception:
		sys.exit(1)

if __name__ == '__main__':	
	parser = argparse.ArgumentParser()

	parser.add_argument(
		'new_name',
		help=('Files matching `file_pattern` will be renamed with this value. '
			  'An incrementing count will also be added.'),
	)
	parser.add_argument(
		'file_pattern',
		help='Files to rename (Regex compatible).',
	)
	parser.add_argument(
		'target_dir',
		help='Directory where the files to rename or copy reside.',
	)
	parser.add_argument(
		'-L', '--log-level',
		help='Set log level: (NOTSET / DEBUG / INFO / WARNING / ERROR / CRITICAL).',
		default='info'
	)
	parser.add_argument(
		'-C', '--copy',
		help='Create a copy: (True / False).',
		default='False'
	)
	
	args = parser.parse_args()

	# Configure logger
	logging.basicConfig(
		level=getattr(logging, args.log_level.upper()),
		format=LOG_FMT_STRING,
	)

	main(args)