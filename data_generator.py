import sys 
import json 
import random 
import boto3
import argparse 
import datetime as dt 
from faker import Faker
import os




class DataGenerator:

    def __init__(self):
        self.userId = 0
        self.channleid = 0
        self.genre = ""
        self.lastactive = ""
        self.title = ""
        self.watchfrequency = 0
        self.etags = ""

    def create_fakeNetflixData(self, fake):
        
        netflixdata = {
            'userId': fake.uuid4(),
            'channelid': fake.uuid4(),
            'genre' : fake.random_element(elements=('Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Thriller')),
            'lastactive': fake.date_time_this_month(),
            'title': fake.random_element(elements=('The Shawshank Redemption', 'The Godfather', 'The Dark Knight', 'The Lord of the Rings', 'Pulp Fiction', 'Schindler\'s List', 'Forrest Gump', 'The Matrix', 'The Silence of the Lambs', 'The Lion King')),
            'watchfrequency': fake.random_int(min=1, max=100),
            'etags': fake.uuid4()
        }

        data = json.dumps(netflixdata)
        return {'Data': bytes(data, 'utf-8'), 'PartitionKey': 'key'}

    def create_fakeNetflixData(self, rate, fake):
        return [self.create_fakeNetflixData(rate ,fake) for _ in range(rate)]

    def dumps_lines(objs):
        for obj in objs:
            yield json.dumps(obj, separators=(',', ':')) + '\n'

def main():
    
    parser = argparse.ArgumentParser(description='Generate fake data for Netflix')
    parser.add_argument('--stream-name', action='store', dest='stream_name', help='Name of the Kinesis stream')
    parser.add_argument('--region', action='store', dest='region', default='us-west-2', help='AWS region')

    args = parser.parse_args()

    #session = boto3.Session(profile_name='SashaKirsten')

    try:
        fake = Faker()

        # Disable proxy for AWS endpoints
        os.environ['NO_PROXY'] = 'amazonaws.com'
        kinesis = boto3.client('kinesis', region_name=args.region)
#         kinesis = boto3.client('kinesis', region_name=args.region, proxies={
#     'http': 'http://127.0.0.1:5500',
#     'https': 'http://127.0.0.1:5500'
# })

        rate = 1

        generator = DataGenerator()

        # Generate fake data
        while True:
            fake_data = generator.create_fakeNetflixData(rate, fake)
            kinesis.put_records(StreamName=args.stream_name, Records=fake_data)

    except Exception as e:
        print(e)
        raise e

if __name__ == '__main__':
    main()