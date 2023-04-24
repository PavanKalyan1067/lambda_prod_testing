import json
import boto3

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('abcd_table')

def lambda_handler(event, context):
    bucket_name = 'prod1data'
    prefix = 'rawdata/'
    
    # List all JSON files in the S3 bucket with the given prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = [file['Key'] for file in response['Contents'] if file['Key'].endswith('.json')]
    
    # Read each JSON file and insert data into DynamoDB table
    for file in files:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file)
        data = obj['Body'].read().decode('utf-8')
            
        transformed_data = []
        for record in json.loads(data):
            transformed_record = {
                'full_name': record['first_name'] + ' ' + record['last_name'],
                'email': record['email']
            }
            transformed_data.append(transformed_record)
    
    # write the transformed data to DynamoDB
        with table.batch_writer() as batch:
            for record in transformed_data:
                batch.put_item(Item=record)
    return {
        'statusCode': 200,
        'body': 'Data successfully transformed and written to DynamoDB.'
    }