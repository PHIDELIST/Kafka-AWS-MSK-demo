import json
import os
from kafka import KafkaProducer

def handler(event, context):
    producer = KafkaProducer(
        bootstrap_servers = os.getenv('MSK_BROKER_URL'), 
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    producer.send('test-topic', {'key': 'value'}) 
    producer.flush()

    return {
        'statusCode': 200,
        'body': json.dumps('Producer Lambda executed successfully')
    }