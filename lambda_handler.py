import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Load the JSON data from S3
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    json_data = json.loads(data)
    
    # Transform the data
    transformed_data = []
    for record in json_data:
        new_record = {}
        new_record['name'] = record['first_name'] + ' ' + record['last_name']
        new_record['email'] = record['email']
        transformed_data.append(new_record)
    
    # Write the transformed data to S3
    transformed_data_str = json.dumps(transformed_data)
    bucket_name = 'prod1data/transformeddata/'
    new_key = 'transformed_data.json'
    s3.put_object(Bucket=bucket_name, Key=new_key, Body=transformed_data_str)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data transformation complete')
    }
