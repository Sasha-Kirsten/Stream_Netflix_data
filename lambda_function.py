import json
import base64 
import boto3

def lambda_handler(event, context):
    print(event)
    try:
        dynamo_db = boto3.resource('dynamodb')
        table = dynamo_db.Table('Aleks-sink-table')

        for record in event["Records"]:
            decoded_data = base64.b64encode(record["kinesis"]["data"]).decode("utf-8")
            print(decoded_data)

            decoded_data_dic = json.loads(decoded_data)

            watch_frequency = decoded_data_dic.get("watchfrequency", 0)


            if isinstance(watch_frequency, int):
                if watch_frequency ==1:
                    decoded_data_dic["reaction"] = "dislike"
                    # elif watch_frequency