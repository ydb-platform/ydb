from collections import defaultdict
import shutil
import subprocess
import signal
import stat
import os
import time
import unittest
import urllib.request
import tarfile
from library.python import resource
import ydb


class Workload(unittest.TestCase):
    def __init__(self, endpoint, database, bootstrap, test_topic_path,
                 target_topic_path, workload_consumer_name, num_workers,
                 duration):
        self.endpoint = endpoint
        self.database = database
        self.bootstrap = bootstrap
        self.test_topic_path = test_topic_path
        self.target_topic_path = target_topic_path
        self.workload_consumer_name = workload_consumer_name
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.num_workers = num_workers
        self.duration = duration
        self.tmp_dirs = []
        self.archive_path = "https://storage.yandexcloud.net/ydb-ci/kafka/jdk-linux-x86_64.yandex.tgz"
        self.jar_path = "https://storage.yandexcloud.net/ydb-ci/kafka/e2e-kafka-api-tests-1.0-with-parameter-choice.jar"
        self._unpack_resource('ydb_cli')

    def _unpack_resource(self, name):
        working_dir = os.path.join(os.getcwd(), "kafka_ydb_cli")
        self.tmp_dirs.append(working_dir)
        os.makedirs(working_dir, exist_ok=True)
        res = resource.find(name)
        path_to_unpack = os.path.join(working_dir, "ydb_cli")
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def loop(self):
        TEST_FILES_DIRECTORY = "./test-files/"
        JAR_FILE_NAME = "e2e-kafka-api-tests-1.0-with-parameter-choice.jar"
        JDK_FILE_NAME = "jdk-linux-x86_64.yandex.tgz"
        if not os.path.exists(TEST_FILES_DIRECTORY):
            os.makedirs(TEST_FILES_DIRECTORY)

        urllib.request.urlretrieve(self.jar_path, TEST_FILES_DIRECTORY + JAR_FILE_NAME)
        urllib.request.urlretrieve(self.archive_path, TEST_FILES_DIRECTORY + JDK_FILE_NAME)
        os.chmod(TEST_FILES_DIRECTORY + JAR_FILE_NAME, 0o777)
        os.chmod(TEST_FILES_DIRECTORY + JDK_FILE_NAME, 0o777)

        tar = tarfile.open(TEST_FILES_DIRECTORY + JDK_FILE_NAME, "r:gz")
        tar.extractall(path=TEST_FILES_DIRECTORY)
        tar.close()

        os.chmod(TEST_FILES_DIRECTORY + 'lib/server/classes.jsa', 0o777)
        os.chmod(TEST_FILES_DIRECTORY + 'lib/server/classes_nocoops.jsa', 0o777)

        java_path = TEST_FILES_DIRECTORY + "/bin/java"
        jar_file_path = TEST_FILES_DIRECTORY + JAR_FILE_NAME

        workloadConsumerName = self.workload_consumer_name

        print("Creating test topic")
        testOptions = [("1", "1"), ("0", "1"), ("0", "0")]
        checkerConsumer = "targetCheckerConsumer"
        self.create_topic(self.test_topic_path, [workloadConsumerName, checkerConsumer] + [f"{checkerConsumer}-{i}" for i in range(len(testOptions))])

        print("Running workload topic run")
        processes = [
            subprocess.Popen([self.cli_path, "-e", self.endpoint, "-d", self.database, "workload", "topic", "run", "write", "--topic", self.test_topic_path, "-s", "10", "--message-rate", "100"])
        ]
        print("NumWorkers: ", self.num_workers)
        print("Bootstrap:", self.bootstrap, "Endpoint:", self.endpoint, "Database:", self.database)

        for i, parameters in enumerate(testOptions):
            use_transactions, use_idempotence = parameters
            targetTopicName = f"{self.target_topic_path}-{i}"
            self.create_topic(targetTopicName, [checkerConsumer, f"{checkerConsumer}-{i}"])
            for j in range(self.num_workers):
                processes.append(subprocess.Popen([java_path, "-jar", jar_file_path, self.bootstrap,
                                                   f"streams-store-{i * self.num_workers + j}", self.test_topic_path,
                                                   targetTopicName, f"workload-consumer-{i}", use_transactions, use_idempotence]))
        processes[0].wait()
        assert processes[0].returncode == 0

        print("-----------------")
        print(f"Waiting for {self.duration} sec")
        time.sleep(self.duration)

        print("Killing processes")
        for i in range(1, len(processes)):
            os.kill(processes[i].pid, signal.SIGKILL)

        topic_description = self.driver.topic_client.describe_topic(self.test_topic_path, include_stats=True)
        print(topic_description)
        messages_info_test = self.read_messages(self.test_topic_path, checkerConsumer)

        for i in range(len(testOptions)):
            messages_info_target = self.read_messages(f"{self.target_topic_path}-{i}", f"{checkerConsumer}-{i}")
            totalMessCountTest = 0
            totalMessCountTarget = 0
            for partitionNum in messages_info_test.keys():
                testPartitionMess = messages_info_test[partitionNum]
                targetPartitionMess = messages_info_target[partitionNum]
                testCount = len(testPartitionMess)
                targetCount = len(targetPartitionMess)
                totalMessCountTest += testCount
                totalMessCountTarget += targetCount

            print(f"target {self.target_topic_path}-{i}. totalMessCountTest = {totalMessCountTest},"
                  "totalMessCountTarget = {totalMessCountTarget}")
            if i >= 1:
                assert totalMessCountTest <= totalMessCountTarget, "Source message count is greater than the target {self.target_topic_path}-{i} topic's message count:" + \
                       f"{totalMessCountTest} and {totalMessCountTarget} respectively."
            else:
                assert totalMessCountTest == totalMessCountTarget, f"Source and target {self.target_topic_path}-{i} topics total messages count are not equal:" + \
                       "{totalMessCountTest} and {totalMessCountTarget} respectively."
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
        for tmp_dir in self.tmp_dirs:
            shutil.rmtree(tmp_dir)
