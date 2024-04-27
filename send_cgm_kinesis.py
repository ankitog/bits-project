import json
import boto3
import uuid
import random
import time
import pandas as pd
def send_data_to_kinesis(data:dict):
    client = boto3.client('kinesis', region_name="us-east-1")
    # data = {
    #     "id": str(uuid.uuid4()),
    #     "latitude": random.uniform(-90, 90),
    #     "longtitude": random.uniform(0, 180)
    # }
        
    response = client.put_record(
        StreamName="cgm-stream",
        PartitionKey="geolocation",
        Data=json.dumps(data)
    )
    
    print(response)

if __name__ == "__main__":
    try:
        cgm_data = pd.read_csv(r'data\Dexcom_010.csv')
        df = cgm_data.drop(cgm_data[cgm_data["Timestamp (YYYY-MM-DDThh:mm:ss)"].isnull()].index)

        while True:
            data = json.loads(df.sample(n=1).to_json(orient="records"))
            send_data_to_kinesis(data[0])
            time.sleep(30)

    except Exception as e:
        print(e)