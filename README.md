# Real-time Cryptocurrency Data Processing Pipeline

A data pipeline for processing cryptocurrency exchange rate data in real-time using Apache Kafka, Apache Spark Structured Streaming, and MySQL.

![Solution Architecture](solution_arch.mmd)

## Project Overview

This project implements both real-time and batch processing of cryptocurrency exchange rate data, enabling performance comparison between the two approaches. The system fetches BTC/USD exchange rates from CoinAPI, processes them through a streaming pipeline, and stores both raw and aggregated data in a MySQL database for analysis.

## Architecture

The data pipeline consists of the following components:

1. **Data Producer**: Fetches real-time cryptocurrency exchange rates from CoinAPI and publishes them to a Kafka topic.
2. **Raw Data Consumer**: Consumes messages from Kafka and stores the raw data in MySQL.
3. **Spark Structured Streaming Processor**: Reads the raw data stream, performs windowed aggregations, and writes results to MySQL.
4. **Performance Analyzer**: Compares the streaming and batch processing approaches.

## Components

### Data Ingestion
- `coinapi_producer.py`: Fetches BTC/USD exchange rates from CoinAPI REST API and publishes to Kafka topic (`raw_coin_data`).

### Data Storage
- `consumer_raw_coin_data.py`: Consumes messages from Kafka and stores them in the MySQL database.
- `consumer_coin_update_count.py`: Tracks the number of updates processed.

### Stream Processing
- `spark_stream_processor.py`: Performs windowed aggregations (count, avg, min, max, sum) on the streaming data using Apache Spark Structured Streaming.

### Analysis
- `compare_stream_vs_batch.py`: Compares performance metrics between streaming and batch processing approaches.

### Database
- `mysql_init.sql`: Contains the database schema definition.

## Setup & Installation

### Prerequisites
- Python 3.7+
- Apache Kafka
- Apache Spark
- MySQL Database
- Java 8+

### Environment Setup

1. Clone the repository:
```
git clone <repository-url>
cd Dbt_proj
```

2. Set up a virtual environment:
```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```
pip install kafka-python pyspark mysql-connector-python requests
```

4. Set up MySQL:
```
mysql -u <username> -p < mysql_init.sql
```

5. Configure environment variables (create a `.env` file with the following):
```
COINAPI_KEY=<your-coinapi-key>
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MYSQL_HOST=localhost
MYSQL_USER=<mysql-username>
MYSQL_PASSWORD=<mysql-password>
MYSQL_DATABASE=coin_data
```

## Usage

1. Start Kafka and Zookeeper servers.

2. Run the data producer:
```
python coinapi_producer.py
```

3. Run the raw data consumer:
```
python consumer_raw_coin_data.py
```

4. Run the Spark streaming processor:
```
python spark_stream_processor.py
```

5. Run the performance comparison:
```
python compare_stream_vs_batch.py
```

## Performance Analysis

The system allows for comparison between:
- Processing latency
- Throughput
- Resource utilization 
- Accuracy of aggregated data

Results are stored in the MySQL database for further analysis.

## Solution Architecture

The solution architecture is visualized in the included Mermaid diagram (`solution_arch.mmd`), showing the flow of data from the CoinAPI through Kafka, Spark Streaming, and into MySQL.

## Future Enhancements

- Support for additional cryptocurrency pairs
- Dashboard for real-time monitoring
- Machine learning integration for price prediction
- Integration with data visualization tools

## License

[MIT License](LICENSE)