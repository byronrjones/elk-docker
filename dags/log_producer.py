from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from confluent_kafka import Producer
from faker import Faker
import logging
from datetime import datetime, timedelta
import random

fake = Faker()
logger = logging.getLogger(__name__)



def produce_logs_test(**context):
    """Produce a single synthetic log entry into Kafka (minimal example)."""
    try:
        # Configure the producer - adjust bootstrap.servers to your Kafka host
        p = Producer({'bootstrap.servers': 'host.docker.internal:29092'})
        msg = fake.text(max_nb_chars=200)
        p.produce('logs', value=msg.encode('utf-8'))
        p.flush(timeout=5)
        logger.info('Produced message to topic "logs"')
    except Exception:
        logger.exception('Failed to produce message')
        raise

def generate_log():
    """Generate synthetic long"""
    methods = ['GET','POST','PUT', 'DELETE']
    endpoints = ['/api/users', '/home', '/about', '/contact', '/services']
    statuses = [200, 301, 302, 400 , 404, 500]

    user_agents = [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 lie Mac OS X)',
        'Mozilla/5.0 (X11; Linux x86_64)',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]

    referrers = ['https://example.com', 'https://google.com', '-', 'https://bing.com', 'https://yahoo.com']

    ip = fake.ipv4()
    timestamp = datetime.now().strftime('%b %d %Y, %H:%M:%S')
    method = random.choice(methods)
    endpoint = random.choice(endpoints)
    status = random.choice(statuses)
    size = random.randint(a=1000, b=15000)
    referrer = random.choice(referrers)
    user_agent = random.choice(user_agents)

    log_entry = (
        f'{ip} - - [{timestamp}] "{method} {endpoint} HTTP/1.1" {status} {size} "{referrer}" {user_agent}'
    )

    return log_entry

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_logs(**context):
    """Produce log entries into Kafka"""
    kafka_config = {'bootstrap.servers': 'host.docker.internal:29092'}

    # Configure the producer - adjust bootstrap.servers to your Kafka host
    producer = Producer(kafka_config)
    topic = 'billion_website_logs'

    for _ in range(15000):
        log = generate_log()    
        try:
            msg = log
            producer.produce(topic, value=log.encode('utf-8'), on_delivery=delivery_report)
            producer.flush(timeout=5)
        except Exception as e:
            logger.error(f'Error producing log: {e}')
            raise

default_args = {
    'owner': 'Data Mastery Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# dag = DAG(
#     dag_id='log_generation_pipeline',
#     default_args=default_args,
#     description='Generate and produce synthetic logs',
#     schedule='*/5 * * * *',
#     start_date=datetime(year=2025, month=1, day=26),
#     catchup=False,
#     tags=['logs', 'kafka', 'production'],
# )

# produce_logs_task = PythonOperator(
#     task_id='generate_and_produce_logs',
#     python_callable=produce_logs,
#     dag=dag,
# )

#test producer
produce_logs()