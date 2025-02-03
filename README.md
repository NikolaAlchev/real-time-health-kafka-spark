# Real-Time Health Data Processing with Kafka and Spark

This project processes health data in real-time using Kafka, PySpark, and machine learning. It streams, transforms, and analyzes health records, predicting potential diabetes risk and sending the results back to Kafka topics for further processing.

## Quick Start

### Start Kafka, Zookeeper, and the Data Producer

Run the provided shell script to set up the environment, start Kafka and Zookeeper via Docker Compose, and launch the producer:

```sh
./start.sh
```

### Run the Spark Consumer

Execute the following command to start the Spark consumer which reads the data, processes it, and outputs predictions:

```sh
spark-submit --master local[4] --jars jars\spark-sql-kafka-0-10_2.12-3.5.4.jar,jars\kafka-clients-3.8.0.jar,jars\spark-streaming-kafka-0-10_2.12-3.5.4.jar,jars\spark-token-provider-kafka-0-10_2.12-3.5.4.jar,jars\commons-pool2-2.11.1.jar consumer.py
```