# Wikimedia Data Pipeline with Kafka and OpenSearch

This project is a data pipeline designed to ingest data from Wikimedia, process it using Apache Kafka, and store it in OpenSearch for further analysis. The pipeline consists of several components including a Kafka producer, Kafka broker, Kafka consumer, and OpenSearch sink.

## System Design

The system design involves the following components:

- **Data Source**: Wikimedia provides the data feed which serves as our primary data source.

- **Kafka Producer**: Responsible for fetching data from Wikimedia and publishing it to Kafka topics.

- **Kafka Broker**: Acts as the intermediary for the data flow, receiving data from producers and serving it to consumers. The Kafka bootstrap server is configured to connect producers and consumers to the Kafka cluster.

- **Kafka Topic**: Data is inserted into Kafka topics with three partitions and one in-sync replica (ISR) for fault tolerance and scalability.

- **Kafka Consumer Logic**: The Kafka consumer logic reads data streams/events from Kafka topics and processes them accordingly.

- **OpenSearch**: Serves as the data sink where processed data is stored for further analysis.

## Setup Instructions

To set up and run the Wikimedia data pipeline:

1. Clone the repository:

2. Install dependencies:

3. Configure Kafka connection settings in `config.ini`.

4. Run the Kafka producer to fetch data from Wikimedia and publish it to Kafka:

5. Implement the Kafka consumer logic to process data from Kafka topics.

6. Configure OpenSearch connection settings in `config.ini`.

7. Develop the data ingestion process from Kafka to OpenSearch using your preferred programming language or tool.

8. Run and test the entire pipeline to ensure data is flowing seamlessly from Wikimedia to OpenSearch.

