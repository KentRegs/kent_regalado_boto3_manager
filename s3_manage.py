from botocore.exceptions import ClientError
import argparse
import logging
import boto3

log = logging.getLogger(__name__)

def create_bucket(name, region=None):
	region = region or 'us-east-2'
	client = boto3.client('s3', region_name=region)
	
	params = {
		'Bucket': name,
		'CreateBucketConfiguration': {
			'LocationConstraint': region,
		}
	}

	try:
		client.create_bucket(**params)
		return True
	except ClientError as err:
		log.error(f'{err} - Params {params}')
		return False

def list_buckets():
	s3 = boto3.resource('s3')
	count = 0

	for bucket in s3.buckets.all():
		print(bucket.name)
		count += 1

	print(f'Found {count} buckets!')

def get_bucket(name, create=False, region=None):
	client = boto3.resource('s3')
	bucket = client.Bucket(name=name)

	if bucket.creation_date:
		log.info(f'Bucket <{name}> was created on {bucket.creation_date}.')
		return bucket		
	else:
		if create:
			create_bucket(name, region=region)
			return get_bucket(name)
		else:
			log.warning(f'Bucket {name} does not exist!')
			return

def create_tempfile(file_name=None, content=None, size=300):
	# Create a temporary text file
	filename = f'{file_name or uuid.uuid4().hex}.txt'

	with open(filename, 'w') as f:
		f.write(f'{(content or "0") * size}')

	return filename

def create_bucket_object(bucket_name, file_path, key_prefix=None):
	"""Create a bucket object
	:params bucket_name: The target bucket
	:params type: str
	:params file_path: The path to the file to be uploaded to the bucket.
	:params type: str
	:params key_prefix: Optional prefix to set in the bucket for the file.
	:params type: str
	"""
	bucket = get_bucket(bucket_name)
	dest = f'{key_prefix or ""}{file_path}'
	bucket_object = bucket.Object(dest)
	bucket_object.upload_file(Filename=file_path)

	return bucket_object

def get_bucket_object(bucket_name, object_key, dest=None, version_id=None):
	"""Download a bucket object
	:params bucket_name: The target bucket
	:params type: str
	:params object_key: The bucket object to get
	:params type: str
	:params dest: Optional location where the downloaded file will stored in your local.
	:params type: str
	:returns: The bucket object and downloaded file path object.
	:rtype: tuple
	"""
	bucket = get_bucket(bucket_name)
	params = {'key': object_key}

	if version_id:
		params['VersionId'] = version_id
	
	bucket_object = bucket.Object(**params)
	dest = Path(f'{dest or ""}')
	file_path = dest.joinpath(PosixPath(object_key).name)
	bucket_object.download_file(f'{file_path}')
	
	return bucket_object, file_path

def enable_bucket_versioning(bucket_name):
	# Enable bucket versioning for the given bucket_name
	bucket = get_bucket(bucket_name)
	versioned = bucket.Versioning()
	versioned.enable()

	return versioned.status

def delete_bucket_objects(bucket_name, key_prefix=None):
	"""Delete all bucket objects including all versions
	of versioned objects.
	"""
	bucket = get_bucket(bucket_name)
	objects = bucket.object_versions

	if key_prefix:
		objects = objects.filter(Prefix=key_prefix)
	else:
		objects = objects.iterator()
		
	targets = [] # This should be a max of 1000
	
	for obj in objects:
		targets.append({
			'Key': obj.object_key,
			'VersionId': obj.version_id,
		})
	
	bucket.delete_objects(Delete={
		'Objects': targets,
		'Quiet': True,
	})

	return len(targets)

def delete_buckets(name=None):
	count = 0

	if name:
		bucket = get_bucket(name)
		if bucket:
			bucket.delete()
			bucket.wait_until_not_exists()
			count += 1
	else:
		count = 0
		client = boto3.resource('s3')

		for bucket in client.buckets.iterator():
			try:
				bucket.delete()
				bucket.wait_until_not_exists()
				count += 1
			except ClientError as err:
				log.warning(f'Bucket {bucket.name}: {err}')

	return count

if __name__ == '__main__':	
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()

	s3_list = subparsers.add_parser('list')
	s3_list.set_defaults(func=list_buckets)

	s3_create = subparsers.add_parser('create')
	s3_create.add_argument('name', help='Bucket name.')
	s3_create.add_argument('--region', help='AWS Region.')
	s3_create.set_defaults(func=create_bucket)

	s3_get = subparsers.add_parser('get')
	s3_get.add_argument('name', help='Bucket name.')
	s3_get.add_argument('--region', help='AWS Region.')
	s3_get.set_defaults(func=get_bucket)

	s3_create_tempf = subparsers.add_parser('create_tempf')
	s3_create_tempf.add_argument('--filename', help='Name of temporary file.')
	s3_create_tempf.add_argument('--content', help='Content of temporary file.')
	s3_create_tempf.set_defaults(func=create_tempfile)

	s3_create_bucket_obj = subparsers.add_parser('create_bucket_obj')
	s3_create_bucket_obj.add_argument('bucket_name', help='The target bucket.')
	s3_create_bucket_obj.add_argument('file_path', help='The path to the file to be uploaded to the bucket.')
	s3_create_bucket_obj.add_argument('--keyprefix', help='Optional prefix to set in the bucket for the file.')
	s3_create_bucket_obj.set_defaults(func=create_bucket_object)	

	s3_get_bucket_obj = subparsers.add_parser('get_bucket_obj')
	s3_get_bucket_obj.add_argument('bucket_name', help='The target bucket.')
	s3_get_bucket_obj.add_argument('object_key', help='The bucket object to get.')
	s3_get_bucket_obj.add_argument('--dest', help='Optional location where the downloaded file will stored in your local.')
	s3_get_bucket_obj.set_defaults(func=get_bucket_object)

	s3_enable_bucket_vrsng = subparsers.add_parser('enable_bucket_vrsng')
	s3_enable_bucket_vrsng.add_argument('bucket_name', help='Bucket name.')
	s3_enable_bucket_vrsng.set_defaults(func=enable_bucket_versioning)

	s3_delete_bucket_obj = subparsers.add_parser('delete_bucket_obj')
	s3_delete_bucket_obj.add_argument('bucket_name', help='Bucket name.')
	s3_delete_bucket_obj.add_argument('--keyprefix', help='Optional prefix to set in the bucket for the file.')
	s3_delete_bucket_obj.set_defaults(func=delete_bucket_objects)

	s3_delete = subparsers.add_parser('delete')
	s3_delete.add_argument('--name', help='Bucket name.')
	s3_delete.set_defaults(func=delete_buckets)

	args = parser.parse_args()

	if hasattr(args, 'func'):
		if args.func.__name__ == 'list_buckets':
			args.func()
		elif args.func.__name__ == 'create_bucket':
			args.func(name=args.name, region=args.region)
		elif args.func.__name__ == 'get_bucket':
			args.func(name=args.name, region=args.region)
		elif args.func.__name__ == 'create_tempfile':
			args.func(file_name=args.filename, content=args.content)
		elif args.func.__name__ == 'create_bucket_object':
			args.func(bucket_name=args.name, file_path=args.path, keyprefix=args.keyprefix)
		elif args.func.__name__ == 'get_bucket_object':
			args.func(bucket_name=args.name, object_key=args.objectkey, dest=args.dest)
		elif args.func.__name__ == 'enable_bucket_versioning':
			args.func(bucket_name=args.bucket_name)
		elif args.func.__name__ == 'delete_bucket_objects':
			args.func(bucket_name=args.bucket_name, keyprefix=args.keyprefix)
		elif args.func.__name__ == 'delete_buckets':
			args.func(name=args.name)