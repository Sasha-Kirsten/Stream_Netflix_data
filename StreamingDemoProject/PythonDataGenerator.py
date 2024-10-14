import random 
import json
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer

#Kafka producer configuration
conf = {
    'bootstrap.servers': "localhost:9092",
    'client.id': "python-producer",
}
producer = Producer(conf)

#List of possible locations, genres and titles for tv-shows
locations = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Spain', 'Italy', 'Japan', 'Australia', 'India', 'Mexico', 'Brazil']
genres = ['Comedy', 'Drama', 'Action', 'Horror', 'Documentary', 'Science Fiction', 'Fantasy', 'Mystery', 'Thriller', 'Romance', 'Crime', 'Adventure', 'Animation']
titles = ['The Simpsons', 'Friends', 'The Office', 'Breaking Bad', 'Game of Thrones', 'Stranger Things', 'The Crown', 'The Mandalorian', 'The Witcher', 'The Big Bang Theory', 'The Walking Dead', 'The Good Place']

#Simulating random user data for Netflix viewing history
def generate_data():
    data = {
        "user_id": random.randint(1000, 9999),
        "location": random.choice(locations),
        "channel_id": random.randint(1, 100),
        "genre": random.choice(genres),
        "last_activity_timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 120))).strftime('%Y-%m-%d %H:%M:%S'),
        "title": random.choice(titles),
        "watch_frequency": random.randint(1, 10),
        "etag": f"{random.randint(10000, 99999)}-{random.randint(10000, 99999)}"
    }
    return json.dumps(data)  # Return the JSON serialized data

#Function to send the data to Kafka
def produce_to_kafka(topic):
    while True:
        netflix_data = generate_data()

        #Produce the message to Kafka
        producer.produce(topic, value=netflix_data)
        print(f"Produced: {netflix_data}")

        #Force a flush to ensure delivery
        producer.flush()

        #Sleep for a random time between 0.5 and 2 seconds
        time.sleep(random.uniform(0.5, 2))

if __name__ == "__main__":
    kafka_topic = "netflix-steam-data"
    produce_to_kafka(kafka_topic)
