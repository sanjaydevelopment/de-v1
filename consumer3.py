from confluent_kafka import Consumer, KafkaError
import json
from pymongo import MongoClient


consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}


mongo_client = MongoClient('mongodb+srv://username:password@cluster0.x5iao9l.mongodb.net/')
db = mongo_client['books'] 
batch_size = 100
consumer = Consumer(consumer_config)
topics = ['high_quality_books', 'technology_books']
consumer.subscribe(topics)

batch = [] 

try:
    while True:
        
        msg = consumer.poll(1.0)  

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break


        received_message = json.loads(msg.value().decode('utf-8'))
        topic_name = msg.topic()
        print(f"Received message: {received_message} from topic {topic_name}")
        batch.append(received_message)
        if len(batch) >= batch_size:
            db[topic_name].insert_many(batch)
            print(f"Inserted {len(batch)} messages into '{topic_name}' collection.")
            batch = [] 

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
    mongo_client.close()
