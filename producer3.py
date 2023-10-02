import pandas as pd
from confluent_kafka import Producer
import json

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}

def create_kafka_producer():
    return Producer(kafka_config)

def publish_to_kafka(producer, topic, message):
    producer.produce(topic, value=message)
    producer.flush() 
    
    

csv_file_path = 'C:\\BooksQuality\\Amazon Books Data.csv'
df = pd.read_csv(csv_file_path)


df = df.dropna(subset=['publish_date', 'rating', 'price'])
df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')
df['publish_year'] = df['publish_date'].dt.year
df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
df['price'] = pd.to_numeric(df['price'].replace('[\$,]', '', regex=True), errors='coerce')


producer = create_kafka_producer()

for index, row in df.iterrows():
    publish_date = row['publish_date']
    rating = row['rating']
    price = row['price']
    book_name = row['title']

    if not pd.isna(publish_date) and not pd.isna(rating) and not pd.isna(price):
        year = publish_date.year
        if year >= 2010 and rating >= 4.5:
            message = {
                'book_name': book_name,
                'rating': rating,
                'publish_year': year,
                'price': price
            }
            json_message = json.dumps(message) 
            publish_to_kafka(producer, 'high_quality_books', json_message.encode('utf-8'))  
            

        if isinstance(book_name, str) and any(keyword in book_name.lower() for keyword in ['technology', 'java', 'python']):
            message = {
                'book_name': book_name,
                'rating': rating,
                'publish_year': year,
                'price': price
            }
            json_message = json.dumps(message)  
            publish_to_kafka(producer, 'technology_books', json_message.encode('utf-8'))  


producer.flush()
