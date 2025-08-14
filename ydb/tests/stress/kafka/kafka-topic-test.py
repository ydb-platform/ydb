from dataclasses import dataclass, field
from collections import defaultdict
from typing import Optional, Sequence
import argparse
import asyncio
import ydb
import unittest
import subprocess
import time
import os
import urllib.request
import sys
import signal

async def connect(endpoint: str, database: str) -> ydb.aio.Driver:
    config = ydb.DriverConfig(endpoint=endpoint, database=database)
    config.credentials = ydb.credentials_from_env_variables()
    driver = ydb.aio.Driver(config)
    await driver.wait(5, fail_fast=True)
    return driver

async def read_messages(driver: ydb.aio.Driver, topic: str, consumer: str):
    async with driver.topic_client.reader(topic, consumer) as reader:
        messages_info = defaultdict(list)
        while True:
            try:
                mess = await asyncio.wait_for(reader.receive_message(), 5)
                messages_info[mess.partition_id].append([mess.partition_id, mess.seqno, mess.created_at])
                reader.commit(mess)
            except asyncio.TimeoutError:
                return messages_info

async def create_topic(driver: ydb.aio.Driver, topic: str, consumers: list[str]):
    try:
        await driver.topic_client.drop_topic(topic)
    except ydb.SchemeError:
        pass

    await driver.topic_client.create_topic(topic, consumers=consumers, min_active_partitions=10)

async def drop_topic(driver: ydb.aio.Driver, topic: str):
    try:
        await driver.topic_client.drop_topic(topic)
    except ydb.SchemeError:
        pass

class MyTestCase(unittest.IsolatedAsyncioTestCase):
    ENDPOINT = "grpc://lbk-dev-4.search.yandex.net:31105"
    DATABASE = "/Root/test"
    BOOTSTRAP = "lbk-dev-4.search.yandex.net:19092"
    TEST_TOPIC_PATH = "test-topic"
    TARGET_TOPIC_PATH = "target-topic"
    WORKLOAD_CONSUMER_NAME = "workload-consumer-0"
    NUM_WORKERS = 1

    async def test(self):

        JAR_FILE_LINK = "https://storage.yandexcloud.net/ydb-ci/kafka/e2e-kafka-api-tests-1.0-SNAPSHOT-all.jar"
        JAR_FILE_NAME = "e2e-kafka-api-tests-1.0-SNAPSHOT-all.jar"
        TEST_FILES_DIRECTORY = "./test-files/"

        print(f"Downloading file: {JAR_FILE_LINK}")

        if not os.path.exists(TEST_FILES_DIRECTORY):
            os.makedirs(TEST_FILES_DIRECTORY)
        urllib.request.urlretrieve(JAR_FILE_LINK, TEST_FILES_DIRECTORY + JAR_FILE_NAME)

        print("Connecting to database. \nEndpoint:", self.ENDPOINT, "\nName:", self.DATABASE)
        driver = await connect(self.ENDPOINT, self.DATABASE)

        workloadConsumerName = self.WORKLOAD_CONSUMER_NAME
        checkerConsumer = "targetCheckerConsumer"

        print("Dropping workload-consumer-0-state-store-changelog topic")
        await drop_topic(driver, "workload-consumer-0-state-store-changelog")

        print("Creating topics")
        await create_topic(driver, self.TEST_TOPIC_PATH, [workloadConsumerName, checkerConsumer])
        await create_topic(driver, self.TARGET_TOPIC_PATH, [checkerConsumer])

        print("Running workload topic run")
        processes = [
            subprocess.Popen(["ydb", "-e", self.ENDPOINT, "-d", self.DATABASE, "workload", "topic", "run", "write", "--topic", self.TEST_TOPIC_PATH, "-s", "10", "--message-rate", "100"])
        ]
        print("NumWorkers: ", self.NUM_WORKERS)
        for i in range(self.NUM_WORKERS):
            processes.append(subprocess.Popen(["ya", "tool", "java", "-jar", "./test-files/e2e-kafka-api-tests-1.0-SNAPSHOT-all.jar", self.BOOTSTRAP, f"streams-store-{i}"]),)
        processes[0].wait()

        print("-----------------")
        print("Waiting for 120 sec")
        time.sleep(120)

        print("Killing processes")
        for i in range(1, self.NUM_WORKERS + 1):
            os.kill(processes[i].pid, signal.SIGKILL)

        topic_description = await driver.topic_client.describe_topic(self.TEST_TOPIC_PATH, include_stats=True)
        print(topic_description)

        messages_info_target = await read_messages(driver, self.TARGET_TOPIC_PATH, checkerConsumer)
        messages_info_test = await read_messages(driver, self.TEST_TOPIC_PATH, checkerConsumer)

        totalMessCountTest = 0
        totalMessCountTarget = 0
        for partitionNum in messages_info_test.keys():
            testPartitionMess = messages_info_test[partitionNum]
            targetPartitionMess = messages_info_target[partitionNum]
            testCount = len(testPartitionMess)
            targetCount = len(targetPartitionMess)
            totalMessCountTest += testCount
            totalMessCountTarget += targetCount

        assert totalMessCountTest == totalMessCountTarget, f"Source and target topics total messages count are not equal: {totalMessCountTest} and {totalMessCountTarget} respectively."
        print(f"Total num of messages: {totalMessCountTest}")
        return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""YDB topic basic example.\n""",
    )
    parser.add_argument("-d", "--database", default="/Root/test", help="Name of the database to use")
    parser.add_argument("-b", "--bootstrap", default="lbk-dev-4.search.yandex.net:19092", help="Bootstrap server")
    parser.add_argument("-e", "--endpoint", default="grpc://lbk-dev-4.search.yandex.net:31105", help="Endpoint url to use")
    parser.add_argument("-source", "--sourcePath", default="test-topic", help="Test topic name")
    parser.add_argument("-target", "--targetPath", default="target-topic", help="Target topic name")
    parser.add_argument("-c", "--consumer", default="workload-consumer-0", help="Consumer name")
    parser.add_argument("-n", "--numWorkers", default=2, help="Number of workers")
    args = parser.parse_args()
    os.environ["YDB_ANONYMOUS_CREDENTIALS"] = "1"
    print(args)
    MyTestCase.DATABASE = args.database
    MyTestCase.ENDPOINT = args.endpoint
    MyTestCase.BOOTSTRAP = args.bootstrap
    MyTestCase.TEST_TOPIC_PATH = args.sourcePath
    MyTestCase.TARGET_TOPIC_PATH = args.targetPath
    MyTestCase.WORKLOAD_CONSUMER_NAME = args.consumer
    MyTestCase.NUM_WORKERS = int(args.numWorkers)
    del sys.argv[1:]
    unittest.main(failfast=True)
