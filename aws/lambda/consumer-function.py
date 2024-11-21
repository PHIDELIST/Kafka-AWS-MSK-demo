import json
from kafka import KafkaConsumer

def handler(event, context):
    # Set up KafkaConsumer with correct MSK broker and topic
    consumer = KafkaConsumer(
        'your-topic-name',  # Replace with your topic name
        bootstrap_servers=['<MSK-BROKER-URL>'],  # Replace with your MSK brokers
        group_id='consumer-group',  # Set a unique consumer group ID
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        print(f"Received message: {message.value}")
        # Process the message

    return {
        'statusCode': 200,
        'body': json.dumps('Consumer Lambda executed successfully')
    }