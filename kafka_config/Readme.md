## Kafka-config

To run:
`docker-compose up`

Then setup topics:
`kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic ORDER_UPDATE`
`kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic NEW_ORDER`
