import json
import os
from kafka import KafkaConsumer

def handler(event, context):
    consumer = KafkaConsumer(
        'test-topic',  
        bootstrap_servers = os.getenv('MSK_BROKER_URL'), 
        group_id='consumer-group', 
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    messages = []
    for message in consumer:
        print(f"Received message: {message.value}")
        messages.append(message.value)
        break
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Consumer Lambda executed successfully',
            'receivedMessages': messages
        })
    }