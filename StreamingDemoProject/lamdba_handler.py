import json 
import boto3
import base64

def lambda_handler(event, context):
    print("Event: ", event)

    try:
    
        dynamo_db = boto3.resource('dynamodb')
        table = dynamo_db.Table('netflix_data')

        for record in event['Records']:
            decoded_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            decoded_data_dic = json.loads(decoded_data)

            #Example transformation 
            watch_frequency = decoded_data_dic.get('watchfrequency', 0)
            if isinstance(watch_frequency, int):
                if watch_frequency < 3:
                    decoded_data_dic['impression'] = 'neutral'
                elif 3 <= watch_frequency < 10:
                    decoded_data_dic['impression'] = 'like'
                else:
                    decoded_data_dic['impression'] = 'favorite'
            else:
                decoded_data_dic['impression'] = 'unknown'
            table.put_item(Item=decoded_data_dic)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e
