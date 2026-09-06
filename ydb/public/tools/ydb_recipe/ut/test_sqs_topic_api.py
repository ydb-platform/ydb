import os

import boto3

from ydb.tests.oss.ydb_sdk_import import ydb


SQS_REGION = 'ru-central1'
SECURITY_TOKEN = 'root@builtin'
TOPIC_NAME = 'sqs-topic-api-test'
CONSUMER_NAME = 'ydb-sqs-consumer'


def test_sqs_topic_api_is_available_through_recipe():
    database = '/' + os.environ['YDB_DATABASE'].strip('/')
    with ydb.Driver(endpoint=os.environ['YDB_ENDPOINT'], database=database) as driver:
        driver.wait(timeout=10, fail_fast=True)
        with ydb.QuerySessionPool(driver) as session_pool:
            session_pool.execute_with_retries("""
                CREATE TOPIC `{}`
                  (CONSUMER `{}`
                    WITH (
                      type = 'shared',
                      keep_messages_order = false
                    )
                  );
            """.format(TOPIC_NAME, CONSUMER_NAME))

    client = boto3.client(
        service_name='sqs',
        aws_access_key_id='unused',
        aws_secret_access_key='unused',
        aws_session_token=SECURITY_TOKEN,
        endpoint_url=os.environ['YDB_HTTP_PROXY_ENDPOINT'] + database,
        region_name=SQS_REGION,
    )

    response = client.get_queue_url(QueueName=TOPIC_NAME)
    assert response['QueueUrl']
