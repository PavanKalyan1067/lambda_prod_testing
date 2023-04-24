import boto3
import json
import os

# set up clients
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    
    # read the data from the source file
    response = s3.get_object(Bucket='prod1data', Key='rawdata/sstudents_data.json')
    data = response['Body'].read().decode('utf-8')
    
    # transform the data
    transformed_data = []
    for record in json.loads(data):
        transformed_record = {
            'full_name': record['first_name'] + ' ' + record['last_name'],
            'email': record['email']
        }
        transformed_data.append(transformed_record)
    
    # write the transformed data to DynamoDB
    table_name = 'abcd_table'
    with dynamodb.batch_writer(TableName=table_name) as batch:
        for record in transformed_data:
            batch.put_item(Item=record)
    
    # return a success response
    return {
        'statusCode': 200,
        'body': 'Data successfully transformed and written to DynamoDB.'
    }
