import json
from kafka import KafkaProducer

def handler(event, context):
    producer = KafkaProducer(
        bootstrap_servers=['<MSK-BROKER-URL>'],  # Replace with your MSK brokers
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Send a message to the MSK topic
    producer.send('<your-topic-name>', {'key': 'value'})  # Replace with your topic and message content
    producer.flush()

    return {
        'statusCode': 200,
        'body': json.dumps('Producer Lambda executed successfully')
    }