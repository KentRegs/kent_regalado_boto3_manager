from botocore.exceptions import ClientError
from pathlib import Path
import subprocess
import requests
import argparse
import logging
import boto3
import json
import csv
import sys
import os

# csv_url = https://raw.githubusercontent.com/woocommerce/woocommerce/master/sample-data/sample_products.csv

# Initialize logger 
LOG_FMT_STRING = (
	'[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s'
)
logging.basicConfig(level=logging.INFO, format=LOG_FMT_STRING)
log = logging.getLogger(__name__)

# create an 'output.csv' file if it does not exist yet 
def checkFile(name, list_path):
	if os.path.isfile(name):
		log.info(f'<{name}> exists')
	else:
		log.info(f'Does not exist. Creating <{name}>...')
		subprocess.run(['touch', name])

# download csv file from user input url
def get_csv(csv_url):
	with requests.Session() as req:
		try:
			response = req.get(csv_url)
			response.raise_for_status()

			return response

		except requests.HTTPError as http_err:
			message = f'HTTP error occurred: {http_err}'
			log.info(message)

			source_application = os.path.basename(__file__)
			temp = {
				"log_level": "ERROR",
				"message": message,
				"details": "",
				"source_application": source_application
			}
			
			payload = json.dumps(temp)
			callLoggingAndNotifAPI(payload)

			# exit program after logging
			sys.exit(1)
		except Exception as err:
			message = f'Error occurred: {err}'
			log.info(message)

			source_application = os.path.basename(__file__)
			temp = {
				"log_level": "ERROR",
				"message": message,
				"details": "",
				"source_application": source_application
			}

			payload = json.dumps(temp)
			callLoggingAndNotifAPI(payload)

			# exit program after logging
			sys.exit(1)
def readAndWrite(args):
	csv_url = args.csv_url
	base_path = '/home/kent/Desktop/python/python_capstone/python_capstone_project/'
	file_name = 'output.csv'

	# check if 'copied_files' directory exists
	# if not, create it
	checkFile(file_name, base_path)

	dir_path = ''.join((base_path, file_name))
	response = get_csv(csv_url)

	# open 'output.csv'
	with open(dir_path, 'w', newline='') as out:	
		reader = csv.DictReader(response.text.strip().split('\n'))
		writer = csv.DictWriter(out, fieldnames=reader.fieldnames)

		# write fieldnames / column names in 'output.csv'
		writer.writeheader()

		# iterate through each row of the CSV file and 
		# write products with categories in 'output.csv'
		for row in reader:
			if row['Categories']:		
				writer.writerow(row)

	# generate payload to be passed as an argument to the 
	# 'callLoggingAndNotifAPI' function
	message = ' '.join(('Done processing CSV with URL:', csv_url))
	source_application = os.path.basename(__file__)
	temp = {
		"log_level": "INFO",
		"message": message,
		"details": "",
		"source_application": source_application
	}

	# log.info(type(temp))	
	# log.info(temp)

	payload = json.dumps(temp)
	callLoggingAndNotifAPI(payload)
	# except Exception as err:
	# 	# generate payload to be passed as an argument to the 
	# 	# 'callLoggingAndNotifAPI' function
	# 	message = ' '.join(('Failed to process CSV with URL:', csv_url))
	# 	source_application = os.path.basename(__file__)
	# 	temp = {
	# 		"log_level": "ERROR",
	# 		"message": message,
	# 		"details": "",
	# 		"source_application": source_application
	# 	}
			
	# 	payload = json.dumps(temp)
	# 	callLoggingAndNotifAPI(payload)

def callLoggingAndNotifAPI(payload):
	log.info('Processing request...')
	
	response = requests.post(
		url="https://qm5etw0909.execute-api.ap-southeast-1.amazonaws.com/default/kent-capstone",
		headers={'x-api-key': 'LKTJdj3oNC6qSjKHeXQb1azDUcSxN1cq1VvrPPls'},
		data=payload
	)

	log.info(f'{response.json()}')

if __name__ == '__main__':	
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'csv_url',
		help=('URL to CSV file.'),
	)

	args = parser.parse_args()


	readAndWrite(args)