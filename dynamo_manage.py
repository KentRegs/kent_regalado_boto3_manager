from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import operator as op
import argparse
import logging
import boto3
import json

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

# create_dynamo_table(
# 	'products',
# 	pk=[
# 		{
# 		'AttributeName': 'category',
# 		'KeyType': 'HASH',
# 		},
# 		{
# 		'AttributeName': 'sku',
# 		'KeyType': 'RANGE',
# 		},
# 	],
# 	pkdef=[
# 		{
# 		'AttributeName': 'category',
# 		'AttributeType': 'S',
# 		},
# 		{
# 		'AttributeName': 'sku',
# 		'AttributeType': 'S',
# 		},
# 	],
# )

def get_dynamo_table(table_name):
	ddb = boto3.resource('dynamodb')

	return ddb.Table(table_name)

def create_product(category, sku, **product):
	table = get_dynamo_table('products_kent')
	keys = {
		'category': category,
		'sku': sku,
	}
	product.update(keys)
	table.put_item(Item=product)

	# print(table)
	# print(table.get_item(Key=keys)['Item'])
	return table.get_item(Key=keys)['Item']

# product = create_product(
# 	'clothing', 'woo-hoodie927',
# 	product_name='Hoodie',
# 	is_published=True,
# 	price=Decimal('44.99'),
# 	in_stock=True
# )

# refactored **item to **product to prevent 
# "botocore.exceptions.ClientError: An error occurred (ValidationException) 
# when calling the UpdateItem operation: Invalid UpdateExpression: Attribute 
# name is a reserved keyword; reserved keyword: item" from occurring
def update_product(category, sku, **product):
	table = get_dynamo_table('products_kent')
	keys = {
		'category': category,
		'sku': sku,
	}
	expr = ', '.join([f'{k}=:{k}' for k in product.keys()])
	vals = {f':{k}': v for k, v in product.items()}
	table.update_item(
		Key=keys,
		UpdateExpression=f'SET {expr}',
		ExpressionAttributeValues=vals
	)

	print(table.get_item(Key=keys)['Item'])
	return table.get_item(Key=keys)['Item']

def delete_product(category, sku):
	table = get_dynamo_table('products_kent')
	keys = {
		'category': category,
		'sku': sku,
	}
	res = table.delete_item(Key=keys)
	if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
		return True
	else:
		log.error(f'There was an error when deleting the product: {res}')
	
	return False

def create_dynamo_items(table_name, products, keys=None):
	table = get_dynamo_table(table_name)
	params = {
		'overwrite_by_pkeys': keys
	} if keys else {}
	with table.batch_writer(**params) as batch:
		for product in products:
			batch.put_item(Item=product)

	return True

def query_products(key_expr, filter_expr=None):
	# Query requires that you provide the key filters
	table = get_dynamo_table('products_kent')
	params = {
		'KeyConditionExpression': key_expr,
	}
	if filter_expr:
		params['FilterExpression'] = filter_expr
	
	res = table.query(**params)
	
	return res['Products']

def scan_products(filter_expr):
	# Scan does not require a key filter. It will go through
	# all items in your table and return all matching items.
	# Use with caution!
	table = get_dynamo_table('products_kent')
	params = {
		'FilterExpression': filter_expr,
	}
	res = table.scan(**params)
	
	return res['Items']

def delete_dynamo_table(table_name):
	table = get_dynamo_table(table_name)
	table.delete()
	table.wait_until_not_exists()

	return True

if __name__ == '__main__':	
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()

	dynamo_create = subparsers.add_parser('create')
	dynamo_create.add_argument('table_name', help='Table name.')
	dynamo_create.add_argument('pk', help='Primary key/s.', type=argparse.FileType('r'))
	dynamo_create.add_argument('pkdef', help='Primary key/s definition/s.', type=argparse.FileType('r'))
	dynamo_create.set_defaults(func=create_dynamo_table)

	dynamo_get = subparsers.add_parser('get')
	dynamo_get.add_argument('table_name', help='Table name.')
	dynamo_get.set_defaults(func=get_dynamo_table)

	dynamo_create_product = subparsers.add_parser('create_product')
	dynamo_create_product.add_argument('category', help='Product category.')
	dynamo_create_product.add_argument('sku', help='Stock-keeping unit.')
	dynamo_create_product.add_argument('product', help='Product/s.', nargs='+')
	dynamo_create_product.set_defaults(func=create_product)	

	dynamo_update_product = subparsers.add_parser('update_product')
	dynamo_update_product.add_argument('category', help='Product category.')
	dynamo_update_product.add_argument('sku', help='Stock-keeping unit.')
	dynamo_update_product.add_argument('product', help='Product/s.', nargs='+')
	dynamo_update_product.set_defaults(func=update_product)	

	dynamo_delete = subparsers.add_parser('delete')
	dynamo_delete.add_argument('category', help='Product category.')
	dynamo_delete.add_argument('sku', help='Stock-keeping unit.')
	dynamo_delete.set_defaults(func=delete_product)	

	dynamo_create_items = subparsers.add_parser('create_items')
	dynamo_create_items.add_argument('table_name', help='Table name.')
	dynamo_create_items.add_argument('products', help='Products.')
	dynamo_create_items.add_argument('--keys', help='Primary keys.')
	dynamo_create_items.set_defaults(func=create_dynamo_items)		

	dynamo_query = subparsers.add_parser('query')
	dynamo_query.add_argument('key_expr', help='Key condition expression.')
	dynamo_query.add_argument('--filterexpr', help='Filter expression.')
	dynamo_query.set_defaults(func=query_products)		

	dynamo_scan = subparsers.add_parser('scan')
	dynamo_scan.add_argument('filter_expr', help='Filter expression.')
	dynamo_scan.set_defaults(func=scan_products)		

	dynamo_delete_table = subparsers.add_parser('delete_table')
	dynamo_delete_table.add_argument('table_name', help='Table name.')
	dynamo_delete_table.set_defaults(func=delete_dynamo_table)

	args = parser.parse_args()

	if hasattr(args, 'func'):
		if args.func.__name__ == 'create_dynamo_table':
			args.func(
				table_name=args.table_name, 
				pk=json.load(args.pk),
				pkdef=json.load(args.pkdef)
			)
		elif args.func.__name__ == 'get_dynamo_table':
			args.func(table_name=args.table_name)
		elif args.func.__name__ == 'create_product':
			args.func(
				category=args.category, 
				sku=args.sku,
				product=args.product
			)
		elif args.func.__name__ == 'update_product':
			args.func(
				category=args.category, 
				sku=args.sku,
				product=args.product
			)
		elif args.func.__name__ == 'delete_product':
			args.func(
				category=args.category, 
				sku=args.sku
			)
		elif args.func.__name__ == 'create_dynamo_items':
			args.func(
				table_name=args.table_name, 
				products=args.products,
				keys=args.keys
			)
		elif args.func.__name__ == 'query_products':
			args.func(
				key_expr=args.key_expr, 
				filter_expr=args.filterexpr
			)
		elif args.func.__name__ == 'scan_products':
			args.func(filter_expr=args.filter_expr)
		elif args.func.__name__ == 'delete_dynamo_table':
			args.func(table_name=args.table_name)
