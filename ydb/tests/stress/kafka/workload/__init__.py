from collections import defaultdict
import subprocess
import signal
import stat
import os
import time
import urllib.request
import unittest
import tempfile
from library.python import resource
import ydb


class Workload(unittest.TestCase):
    def __init__(self, endpoint, database, bootstrap, test_topic_path, target_topic_path, workload_consumer_name, num_workers, duration):
        self.endpoint = endpoint
        self.database = database
        self.bootstrap = bootstrap
        self.test_topic_path = test_topic_path
        self.target_topic_path = target_topic_path
        self.workload_consumer_name = workload_consumer_name
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.num_workers = num_workers
        self.duration = duration
        self._unpack_resource('ydb_cli')
        self._unpack_resource("e2e-kafka-api-tests-1.0-SNAPSHOT-all.jar")

    def _unpack_resource(self, name):
        self.working_dir = os.path.join(tempfile.gettempdir(), name)
        os.makedirs(self.working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        if name == "ydb_cli":
            self.cli_path = path_to_unpack
        elif name == "e2e-kafka-api-tests-1.0-SNAPSHOT-all.jar":
            self.jar_path = path_to_unpack


    def loop(self):
        workloadConsumerName = self.workload_consumer_name
        checkerConsumer = "targetCheckerConsumer"

        print("Dropping workload-consumer-0-state-store-changelog topic")
        print("Driver loop: ", self.driver)
        self.drop_topic("workload-consumer-0-state-store-changelog")

        print("Creating topics")
        self.create_topic(self.test_topic_path, [workloadConsumerName, checkerConsumer])
        self.create_topic(self.target_topic_path, [checkerConsumer])

        print("Running workload topic run")
        processes = [
            subprocess.Popen([self.cli_path, "-e", self.endpoint, "-d", self.database, "workload", "topic", "run", "write", "--topic", self.test_topic_path, "-s", "10", "--message-rate", "100"])
        ]
        print("NumWorkers: ", self.num_workers)
        for i in range(self.num_workers):
            processes.append(subprocess.Popen(["ya", "tool", "java", "-jar", self.jar_path, self.bootstrap, f"streams-store-{i}"]),)
        processes[0].wait()

        print("-----------------")
        print(f"Waiting for {self.duration} sec")
        time.sleep(self.duration)

        print("Killing processes")
        for i in range(1, self.num_workers + 1):
            os.kill(processes[i].pid, signal.SIGKILL)

        topic_description = self.driver.topic_client.describe_topic(self.test_topic_path, include_stats=True)
        print(topic_description)

        messages_info_target = self.read_messages(self.target_topic_path, checkerConsumer)
        messages_info_test = self.read_messages(self.test_topic_path, checkerConsumer)

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

    def read_messages(self, topic: str, consumer: str):
        with self.driver.topic_client.reader(topic, consumer) as reader:
            messages_info = defaultdict(list)
            while True:
                try:
                    mess = reader.receive_message(timeout=1)
                    messages_info[mess.partition_id].append([mess.partition_id, mess.seqno, mess.created_at])
                    reader.commit(mess)
                except TimeoutError:
                    print("Have no new messages in a second")
                    return messages_info

    def create_topic(self, topic: str, consumers: list[str]):
        try:
            self.driver.topic_client.drop_topic(topic)
        except ydb.SchemeError:
            pass

        self.driver.topic_client.create_topic(topic, consumers=consumers, min_active_partitions=10)

    def drop_topic(self, topic: str):
        try:
            self.driver.topic_client.drop_topic(topic)
        except ydb.SchemeError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver is not None:
            self.driver.stop()
