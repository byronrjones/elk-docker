from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException, KafkaError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import logging
import boto3
import re

logger = logging.getLogger(__name__)

def parse_log_entry(log_entry):
    log_pattern = r'(?P<ip>[\d\.]+) - - \[(?P<timestamp>.*?)\] "(?P<method>\w+) (?P<endpoint>[\w/]+) (?P<protocol>[\w/\.]+)'
    match = re.match(log_pattern, log_entry)
    if not match:
        logger.warning(f'Invalid log format: {log_entry}')
        return None

    data = match.groupdict()

    try:
        parsed_timestamp = datetime.strptime(data['timestamp'], '%b %d %Y, %H:%M:%S')
        data['@timestamp'] = parsed_timestamp.isoformat()
    except ValueError:
        logger.error(f"Timestamp parsing error: {data['timestamp']}")
        return None

    return data

def consume_and_index_logs(**context):
    #Kafka Connection
    # secrets = get_secret('MWAA_Secrets_v2')
    kafka_config = {
        'bootstrap.servers': 'host.docker.internal:29092',
        # 'security.protocol': 'SASL_SSL',
        # 'sasl.mechanisms': 'PLAIN',
        # 'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        # 'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        # 'session.timeout.ms': 50000,
        'group.id': 'mwaa_log_indexer',
        'auto.offset.reset': 'earliest'
    }

    #Elasticsearch Connection
    # es_config = {
    #     'hosts': ['http://localhost:9200'],
    #     basic_auth=("elastic","changeme")
    #     # 'api_key': secrets['ELASTICSEARCH_API_KEY']
    # }

    consumer = Consumer(kafka_config)
    es = Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic","changeme"),
        #headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"}
    )

    topic = 'billion_website_logs'
    consumer.subscribe([topic])
    logger.info(f'Subscribed to topics: {[topic]}')

    #check if index exists. Create topic if it does not exist

    try:
        index_name = 'billion_website_logs'
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            logger.info(f'Created index: {index_name}')
    except Exception as e:
        logger.error(f'Failed to index log: {e}')

    try:
        logs = []
        while True:
            msg = consumer.poll(timeout=10.0)
            logger.info(f'consumer polled: {msg}')
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                logger.error(f'Error consuming from Kafka: {msg.error()}')
                raise KafkaException(msg.error())

            # process the message
            log_entry = msg.value().decode('utf-8')
            parsed_log = parse_log_entry(log_entry)
            
            if parsed_log:
                logs.append(parsed_log)

            # index when 15_000 logs are collected
            if len(logs) >= 15000:
                actions = [
                    {
                        '_op_type': 'create',
                        '_index': index_name,
                        '_source': log
                    }
                    for log in logs

                ]
                success, failed = bulk(es, actions, refresh=True)
                logger.info(f'Indexed {success} logs, {len(failed)} failed')
                logs = []

        # index any remaining logs after exiting loop
        if logs:
            actions = [
                {
                    '_op_type': 'create',
                    '_index': index_name,
                    '_source': log
                }
                for log in logs
            ]
            success, failed = bulk(es, actions, refresh=True)
            logger.info(f'Indexed {success} logs, {len(failed)} failed')
            logs = []
    except Exception as e:
        logger.error(f'Failed to index log: {e}')
    
    try:
        # index any remaining logs
        if logs:
            actions = [
                {
                    '_op_type': 'create',
                    '_index': index_name,
                    '_source': log
                }
                for log in logs

            ]
            bulk(es, actions, refresh=True)
    except Exception as e:
        logger.error(f'Log processing error: {e}')
    finally:
        consumer.close()
        es.close()

default_args = {
    'owner': 'Data Mastery Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# dag = DAG(
#     dag_id='log_consumer_pipeline',
#     default_args=default_args,
#     description='Generate and produce synthetic logs',
#     schedule_interval='*/5 * * * *',
#     start_date=datetime(year=2025, month=1, day=26),
#     catchup=False,
#     tags=['logs', 'kafka', 'production'],
# )

# consume_logs_task = PythonOperator(
#     task_id='generate_and_produce_logs',
#     python_callable=consume_and_index_logs,
#     dag=dag,
# )
consume_and_index_logs()