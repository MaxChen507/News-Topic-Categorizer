# Scalable-News-Categorizer

** To start Servers:
~/Documents/kafka_2.11-2.2.0$ bin/zookeeper-server-start.sh config/zookeeper.properties 

~/Documents/kafka_2.11-2.2.0$ bin/kafka-server-start.sh config/server.properties 


** Produce News Articles API --topic is guardian3
~/Documents$ python3 stream_producer.py ae090bf7-d214-45dc-b156-6758828608ad 2018-11-3 2018-11-4


** Intial Command to fill training data (change topic of producer to guardian2)
~/Documents$ kafka_2.11-2.2.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic guardian2 --from-beginning &> train.txt


** Run Stream Pipeline
~/Documents$ spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 stream_pipeline.py localhost:9092 guardian3 &> result.txt

