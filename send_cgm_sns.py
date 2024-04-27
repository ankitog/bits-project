import json
import boto3
import base64

# Initialize AWS clients
kinesis_client = boto3.client('kinesis')
sns_client = boto3.client('sns')

# Define the SNS topic ARN and glucose threshold
sns_topic_arn = 'arn:aws:sns:us-east-1:610779199836:cgm-event'
glucose_threshold = 100  # Set the desired threshold value

def lambda_handler(event, context):
    # Process each record in the Kinesis event
    
    value = []
    for record in event['Records']:
        print(f"RECORD MONITORING: {record}")
        # payload = json.loads(record['kinesis']['data'])
        payload = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))
        print(f"RECORD MONITORING PAYLOAD: {payload}")
        glucose_value = payload.get('Glucose Value (mg/dL)')

        # Check if the glucose value exceeds the threshold
        if glucose_value and glucose_value > glucose_threshold:
            # Prepare the notification message
            patient_name = payload.get('Patient Name', 'Shashank')
            message = f"Glucose Alert for {patient_name}\n\nCurrent Glucose Level: {glucose_value} mg/dL\nThreshold Exceeded: {glucose_threshold} mg/dL"

            # Publish the notification to SNS
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=message,
                Subject='Glucose Monitoring Alert'
            )
            print(f"Notification sent for patient {patient_name} with glucose value {glucose_value}")

    return {
        'statusCode': 200,
        'body': 'Glucose data processed successfully'
    }