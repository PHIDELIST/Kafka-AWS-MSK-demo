#!/bin/bash

docker exec -it broker-1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic test-topics \
  --bootstrap-server broker-1:19092 \
  --partitions 1 \
  --replication-factor 1

echo "available topic:"
docker exec -it broker-1  /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server broker-1:19092
