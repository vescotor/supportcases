import json
import os
import boto3
import json
import csv

#grab environment variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
runtime = boto3.client('sagemaker-runtime')

def lambda_handler(event, context):
    # TODO implement
    data = json.loads(json.dumps(event))
    payload = data['data']

    
    response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME, ContentType = "application/csv", Body={payload})
    
    print(response)
    result = json.loads(response['Body'].read().decode())
    print(result)
    return result