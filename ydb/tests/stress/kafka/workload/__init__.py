from collections import defaultdict
from contextlib import ExitStack
import shutil
import subprocess
import signal
import stat
import os
import textwrap
import time
import unittest
import urllib.request
import tarfile
from library.python import resource
import ydb


SOURCE_TOPIC_PARTITIONS = 10


SOURCE_SECONDS = 10
SOURCE_MESSAGE_RATE = 100
SOURCE_MESSAGE_COUNT = SOURCE_SECONDS * SOURCE_MESSAGE_RATE

KAFKA_BATCH_PRODUCER_JAVA = textwrap.dedent("""
    import java.nio.charset.StandardCharsets;
    import java.util.Properties;
    import java.util.concurrent.atomic.AtomicReference;

    import org.apache.kafka.clients.producer.Callback;
    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerConfig;
    import org.apache.kafka.clients.producer.ProducerRecord;
    import org.apache.kafka.clients.producer.RecordMetadata;
    import org.apache.kafka.common.serialization.ByteArraySerializer;
    import org.apache.kafka.common.serialization.StringSerializer;

    public class KafkaBatchProducer {
        public static void main(String[] args) throws Exception {
            String bootstrap = args[0];
            String topic = args[1];
            int messageCount = Integer.parseInt(args[2]);
            int messageSize = Integer.parseInt(args[3]);
            int batchSize = Integer.parseInt(args[4]);
            int lingerMs = Integer.parseInt(args[5]);
            String compressionType = args[6];
            String transactionalId = args[7];
            String transactionMode = args[8];
            String payloadPrefix = args[9];
            int keyCount = Integer.parseInt(args[10]);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, "10");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(batchSize));
            props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(lingerMs));
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "180000");
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");

            boolean useTransactions = !"none".equals(transactionalId);
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(useTransactions));
            if (useTransactions) {
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
                props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
                props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "120000");
            }

            AtomicReference<Exception> error = new AtomicReference<>();
            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        error.compareAndSet(null, exception);
                    }
                }
            };

            boolean transactionStarted = false;
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
            try {
                if (useTransactions) {
                    producer.initTransactions();
                    producer.beginTransaction();
                    transactionStarted = true;
                }

                for (int i = 0; i < messageCount; ++i) {
                    producer.send(
                        new ProducerRecord<>(
                            topic,
                            "key-" + (i % keyCount),
                            makePayload(payloadPrefix, i, messageSize)),
                        callback);
                }

                producer.flush();
                Exception exception = error.get();
                if (exception != null) {
                    throw exception;
                }

                if ("commit".equals(transactionMode)) {
                    producer.commitTransaction();
                    transactionStarted = false;
                } else if ("abort".equals(transactionMode)) {
                    producer.abortTransaction();
                    transactionStarted = false;
                } else if (!"none".equals(transactionMode)) {
                    throw new IllegalArgumentException("Unknown transaction mode: " + transactionMode);
                }
            } catch (Exception e) {
                if (useTransactions && transactionStarted) {
                    try {
                        producer.abortTransaction();
                    } catch (Exception abortError) {
                        e.addSuppressed(abortError);
                    }
                }
                throw e;
            } finally {
                producer.close();
            }

            System.out.println(
                "KafkaBatchProducer wrote " + messageCount + " messages to " + topic
                    + ", transaction_mode=" + transactionMode);
        }

        private static byte[] makePayload(String prefix, int index, int messageSize) {
            String seed = prefix + "-message-" + index + "-";
            StringBuilder builder = new StringBuilder(seed);
            while (builder.length() < messageSize) {
                builder.append(seed);
            }
            return builder.substring(0, messageSize).getBytes(StandardCharsets.UTF_8);
        }
    }
""").strip()


class Workload(unittest.TestCase):
    def __init__(self, endpoint, database, bootstrap, test_topic_path,
                 target_topic_path, workload_consumer_name, num_workers,
                 duration, source_writer="topic"):
        self.endpoint = endpoint
        self.database = database
        self.bootstrap = bootstrap
        self.test_topic_path = test_topic_path
        self.target_topic_path = target_topic_path
        self.workload_consumer_name = workload_consumer_name
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.driver.wait(timeout=30)
        self.num_workers = num_workers
        self.duration = duration
        self.source_writer = source_writer
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
        if os.path.exists(TEST_FILES_DIRECTORY):
            shutil.rmtree(TEST_FILES_DIRECTORY)
        if not os.path.exists(TEST_FILES_DIRECTORY):
            os.makedirs(TEST_FILES_DIRECTORY)

        urllib.request.urlretrieve(self.jar_path, TEST_FILES_DIRECTORY + JAR_FILE_NAME)
        urllib.request.urlretrieve(self.archive_path, TEST_FILES_DIRECTORY + JDK_FILE_NAME)
        os.chmod(TEST_FILES_DIRECTORY + JAR_FILE_NAME, 0o777)
        os.chmod(TEST_FILES_DIRECTORY + JDK_FILE_NAME, 0o777)

        tar = tarfile.open(TEST_FILES_DIRECTORY + JDK_FILE_NAME, "r:gz")
        tar.extractall(path=TEST_FILES_DIRECTORY, filter="data")
        tar.close()

        os.chmod(TEST_FILES_DIRECTORY + 'lib/server/classes.jsa', 0o777)
        os.chmod(TEST_FILES_DIRECTORY + 'lib/server/classes_nocoops.jsa', 0o777)

        java_path = TEST_FILES_DIRECTORY + "/bin/java"
        jar_file_path = TEST_FILES_DIRECTORY + JAR_FILE_NAME

        if self.source_writer == "kafka-direct":
            self.run_direct_kafka_batch_tests(java_path, jar_file_path)
            return

        workloadConsumerName = self.workload_consumer_name

        print("Creating test topic")
        testOptions = [
            ("1", "1", True),
            ("0", "1", False),
            ("0", "0", False),
        ]
        if self.source_writer == "kafka":
            testOptions = [
                ("0", "0", False),
            ]
        checkerConsumer = "targetCheckerConsumer"
        stream_consumers = [f"workload-consumer-{i}" for i in range(len(testOptions))]
        self.create_topic(
            self.test_topic_path,
            list(dict.fromkeys(
                [workloadConsumerName, checkerConsumer] + stream_consumers + [
                    f"{checkerConsumer}-{i}" for i in range(len(testOptions))
                ]
            )),
        )

        processes = []
        source_process = None
        print("NumWorkers: ", self.num_workers)
        print("Bootstrap:", self.bootstrap, "Endpoint:", self.endpoint, "Database:", self.database)
        self.clean_streams_state_dirs(len(testOptions) * self.num_workers)

        target_topic_names = []
        for i, parameters in enumerate(testOptions):
            targetTopicName = f"{self.target_topic_path}-{i}"
            self.create_topic(targetTopicName, [checkerConsumer, f"{checkerConsumer}-{i}"])
            target_topic_names.append(targetTopicName)

        try:
            bootstrap = self.kafka_bootstrap()
            for i, parameters in enumerate(testOptions):
                use_transactions, use_idempotence, _ = parameters
                targetTopicName = target_topic_names[i]
                for j in range(self.num_workers):
                    label = (
                        f"target={targetTopicName}, worker={j}, "
                        f"use_transactions={use_transactions}, use_idempotence={use_idempotence}"
                    )
                    processes.append((label, subprocess.Popen([
                        java_path,
                        "-jar",
                        jar_file_path,
                        bootstrap,
                        f"streams-store-{i * self.num_workers + j}",
                        self.test_topic_path,
                        targetTopicName,
                        f"workload-consumer-{i}",
                        use_transactions,
                        use_idempotence,
                    ], start_new_session=True)))
                    # Let the first worker join the group before the second one triggers a rebalance.
                    if j + 1 < self.num_workers:
                        time.sleep(2)

            print("Waiting for Kafka Streams startup")
            time.sleep(10)

            source_process = self.start_source_writer(java_path, jar_file_path)
            source_process.wait()
            assert source_process.returncode == 0

            print("-----------------")
            messages_info_test = self.read_messages(
                self.test_topic_path,
                checkerConsumer,
                expected_count=None,
                timeout=60,
            )
            source_count = self.count_messages(messages_info_test)
            print(f"Source topic has {source_count} readable messages")
            print(f"Waiting up to {self.duration} sec for readable target topic messages")
            messages_info_targets = self.read_messages_from_topics(
                [
                    (f"{self.target_topic_path}-{i}", f"{checkerConsumer}-{i}")
                    for i in range(len(testOptions))
                ],
                expected_count=source_count,
                timeout=self.duration,
                processes=processes,
            )
        finally:
            print("Killing processes")
            if source_process is not None:
                self._kill_process_tree(source_process)
                try:
                    source_process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    print(f"Source process {source_process.pid} did not terminate in time")
            for _, process in processes:
                self._kill_process_tree(process)

            for _, process in processes:
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    print(f"Process {process.pid} did not terminate in time")

        topic_description = self.driver.topic_client.describe_topic(self.test_topic_path, include_stats=True)
        print(topic_description)

        for i in range(len(testOptions)):
            _, _, expect_exact = testOptions[i]
            messages_info_target = messages_info_targets[i]
            totalMessCountTest = self.count_messages(messages_info_test)
            totalMessCountTarget = self.count_messages(messages_info_target)

            print(f"target {self.target_topic_path}-{i}. totalMessCountTest = {totalMessCountTest}, "
                  f"totalMessCountTarget = {totalMessCountTarget}")
            if expect_exact:
                assert totalMessCountTest == totalMessCountTarget, (
                    f"Source and target {self.target_topic_path}-{i} topics total messages count are not "
                    f"equal: {totalMessCountTest} and {totalMessCountTarget} respectively."
                )
            else:
                assert totalMessCountTest <= totalMessCountTarget, (
                    f"Source message count is greater than the target {self.target_topic_path}-{i} topic's "
                    f"message count: {totalMessCountTest} and {totalMessCountTarget} respectively."
                )
        print(f"Total num of messages: {totalMessCountTest}")
        return

    def run_direct_kafka_batch_tests(self, java_path, jar_file_path):
        print("Running direct Kafka batch producer scenarios")
        self.compile_kafka_batch_producer(java_path, jar_file_path)

        scenarios = [
            {
                "name": "plain",
                "topic": self.test_topic_path,
                "consumer": "batchCheckerConsumer-plain",
                "transactional_id": "none",
                "transaction_mode": "none",
                "message_count": SOURCE_MESSAGE_COUNT,
                "expected_count": SOURCE_MESSAGE_COUNT,
                "batch_size": 32768,
                "linger_ms": 100,
                "key_count": 16,
            },
            {
                "name": "transaction-commit",
                "topic": f"{self.target_topic_path}-tx-commit",
                "consumer": "batchCheckerConsumer-tx-commit",
                "transactional_id": "stress-batch-tx-commit",
                "transaction_mode": "commit",
                "message_count": 64,
                "expected_count": 64,
                "batch_size": 262144,
                "linger_ms": 30000,
                "key_count": 1,
            },
            {
                "name": "transaction-abort",
                "topic": f"{self.target_topic_path}-tx-abort",
                "consumer": "batchCheckerConsumer-tx-abort",
                "transactional_id": "stress-batch-tx-abort",
                "transaction_mode": "abort",
                "message_count": 64,
                "expected_count": 0,
                "batch_size": 262144,
                "linger_ms": 30000,
                "key_count": 1,
            },
        ]

        for scenario in scenarios:
            print(f"Creating topic for direct Kafka batch scenario: {scenario['name']}")
            self.create_topic(scenario["topic"], [scenario["consumer"]])

        for scenario in scenarios:
            print(f"Writing direct Kafka batch scenario: {scenario['name']}")
            self.run_kafka_batch_producer(
                java_path,
                jar_file_path,
                scenario["topic"],
                scenario["transactional_id"],
                scenario["transaction_mode"],
                scenario["name"],
                scenario["message_count"],
                scenario["batch_size"],
                scenario["linger_ms"],
                scenario["key_count"],
            )

            if scenario["expected_count"] == 0:
                self.assert_no_messages(scenario["topic"], scenario["consumer"], timeout=self.duration)
                print(f"Scenario {scenario['name']} produced no readable messages as expected")
                continue

            expected_prefix = scenario["name"].encode("utf-8")
            if scenario["transaction_mode"] == "none":
                messages_info = self.read_messages_until_quiet(
                    scenario["topic"],
                    scenario["consumer"],
                    min_count=scenario["expected_count"],
                    timeout=self.duration,
                    quiet_timeout=10,
                    expected_prefix=expected_prefix,
                )
            else:
                messages_info = self.read_messages(
                    scenario["topic"],
                    scenario["consumer"],
                    expected_count=scenario["expected_count"],
                    timeout=self.duration,
                    expected_prefix=expected_prefix,
                )
            total_count = self.count_messages(messages_info)
            self.assert_payload_indexes(
                messages_info,
                expected_prefix,
                scenario["expected_count"],
                allow_duplicates=scenario["transaction_mode"] == "none",
            )
            if scenario["transaction_mode"] == "none":
                assert total_count >= scenario["expected_count"], (
                    f"Scenario {scenario['name']} expected at least {scenario['expected_count']} readable messages, "
                    f"got {total_count}"
                )
            else:
                assert total_count == scenario["expected_count"], (
                    f"Scenario {scenario['name']} expected {scenario['expected_count']} readable messages, "
                    f"got {total_count}"
                )
            print(f"Scenario {scenario['name']} read {total_count} messages")

    def compile_kafka_batch_producer(self, java_path, jar_file_path):
        producer_class_dir = "./kafka-batch-producer"
        self.compile_java_source(
            java_path,
            jar_file_path,
            producer_class_dir,
            "KafkaBatchProducer",
            KAFKA_BATCH_PRODUCER_JAVA,
        )

    def run_kafka_batch_producer(
            self,
            java_path,
            jar_file_path,
            topic,
            transactional_id,
            transaction_mode,
            payload_prefix,
            message_count,
            batch_size,
            linger_ms,
            key_count):
        producer_class_dir = "./kafka-batch-producer"
        producer_command = [
            java_path,
            "-cp",
            f"{jar_file_path}:{producer_class_dir}",
            "KafkaBatchProducer",
            self.kafka_bootstrap(),
            topic,
            str(message_count),
            "256",
            str(batch_size),
            str(linger_ms),
            "none",
            transactional_id,
            transaction_mode,
            payload_prefix,
            str(key_count),
        ]
        print("Kafka batch producer command:", producer_command)
        subprocess.run(producer_command, check=True, text=True)

    def start_source_writer(self, java_path, jar_file_path):
        if self.source_writer == "topic":
            return self.start_topic_source_writer()
        if self.source_writer == "kafka":
            return self.start_kafka_source_writer(java_path, jar_file_path)
        raise ValueError(f"Unknown source writer: {self.source_writer}")

    def start_topic_source_writer(self):
        print("Running workload topic run")
        write_command = [
            self.cli_path, "-e", self.endpoint, "-d", self.database,
            "workload", "topic", "run", "write",
            "--topic", self.test_topic_path,
            "-s", str(SOURCE_SECONDS),
            "--message-rate", str(SOURCE_MESSAGE_RATE),
        ]
        print("Write command:", write_command)
        return subprocess.Popen(write_command, start_new_session=True)

    def start_kafka_source_writer(self, java_path, jar_file_path):
        print("Running Kafka batch producer source writer")
        self.compile_kafka_batch_producer(java_path, jar_file_path)
        producer_class_dir = "./kafka-batch-producer"
        producer_command = [
            java_path,
            "-cp",
            f"{jar_file_path}:{producer_class_dir}",
            "KafkaBatchProducer",
            self.kafka_bootstrap(),
            self.test_topic_path,
            str(SOURCE_MESSAGE_COUNT),
            "256",
            "32768",
            "100",
            "none",
            "none",
            "none",
            "kafka-source",
            "16",
        ]
        print("Kafka source writer command:", producer_command)
        return subprocess.Popen(producer_command, start_new_session=True)

    def clean_streams_state_dirs(self, count):
        for i in range(count):
            state_dir = f"streams-store-{i}"
            if os.path.exists(state_dir):
                shutil.rmtree(state_dir)

    def compile_java_source(self, java_path, jar_file_path, class_dir, class_name, source):
        os.makedirs(class_dir, exist_ok=True)
        source_path = os.path.join(class_dir, f"{class_name}.java")
        with open(source_path, "w") as out:
            out.write(source)

        javac_path = os.path.join(os.path.dirname(java_path), "javac")
        subprocess.run([
            javac_path,
            "-cp",
            jar_file_path,
            source_path,
        ], check=True, text=True)

    def kafka_bootstrap(self):
        bootstrap = self.bootstrap
        for prefix in ("http://", "https://"):
            if bootstrap.startswith(prefix):
                return bootstrap[len(prefix):]
        return bootstrap

    def _kill_process_tree(self, process):
        if process.poll() is not None:
            return
        try:
            pgid = os.getpgid(process.pid)
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError as e:
            print(f"Failed to kill process group for pid {process.pid}: {e}")
            try:
                os.kill(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def assert_no_messages(self, topic: str, consumer: str, timeout=1):
        deadline = time.time() + timeout
        with self.driver.topic_client.reader(topic, consumer) as reader:
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return
                try:
                    mess = reader.receive_message(timeout=min(1, remaining))
                    raise AssertionError(
                        f"{topic} exposed an unexpected message after aborted transaction: "
                        f"partition_id={mess.partition_id}, seqno={mess.seqno}"
                    )
                except TimeoutError:
                    pass

    def read_messages_until_quiet(
            self,
            topic: str,
            consumer: str,
            min_count: int,
            timeout: int,
            quiet_timeout: int,
            expected_prefix=None):
        with self.driver.topic_client.reader(topic, consumer) as reader:
            messages_info = defaultdict(list)
            total_count = 0
            deadline = time.time() + timeout
            quiet_deadline = None
            while True:
                now = time.time()
                if now >= deadline:
                    break
                if quiet_deadline is not None and now >= quiet_deadline:
                    break

                receive_timeout = min(1, deadline - now)
                if quiet_deadline is not None:
                    receive_timeout = min(receive_timeout, quiet_deadline - now)

                try:
                    mess = reader.receive_message(timeout=receive_timeout)
                    data = bytes(mess.data)
                    if expected_prefix is not None and not data.startswith(expected_prefix):
                        raise AssertionError(
                            f"{topic} exposed message with unexpected payload prefix: "
                            f"partition_id={mess.partition_id}, seqno={mess.seqno}"
                        )
                    messages_info[mess.partition_id].append([mess.partition_id, mess.seqno, mess.created_at, data])
                    total_count += 1
                    reader.commit(mess)
                    if total_count >= min_count:
                        quiet_deadline = time.time() + quiet_timeout
                except TimeoutError:
                    if total_count >= min_count:
                        quiet_deadline = time.time() + quiet_timeout if quiet_deadline is None else quiet_deadline

            if total_count < min_count:
                raise AssertionError(
                    f"{topic} did not expose at least {min_count} readable messages: got {total_count}"
                )
            return messages_info

    def read_messages(self, topic: str, consumer: str, expected_count=None, timeout=1, expected_prefix=None):
        with self.driver.topic_client.reader(topic, consumer) as reader:
            messages_info = defaultdict(list)
            total_count = 0
            deadline = time.time() + timeout
            while expected_count is None or total_count < expected_count:
                receive_timeout = 1
                if expected_count is not None:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                    receive_timeout = min(receive_timeout, remaining)
                try:
                    mess = reader.receive_message(timeout=receive_timeout)
                    data = bytes(mess.data)
                    if expected_prefix is not None and not data.startswith(expected_prefix):
                        raise AssertionError(
                            f"{topic} exposed message with unexpected payload prefix: "
                            f"partition_id={mess.partition_id}, seqno={mess.seqno}"
                        )
                    messages_info[mess.partition_id].append([mess.partition_id, mess.seqno, mess.created_at, data])
                    total_count += 1
                    reader.commit(mess)
                except TimeoutError:
                    if expected_count is None:
                        print("Have no new messages in a second")
                        return messages_info
            if expected_count is not None and total_count < expected_count:
                raise AssertionError(f"{topic} did not expose {expected_count} readable messages: got {total_count}")
            return messages_info

    def read_messages_from_topics(self, topics, expected_count, timeout, processes=None):
        deadline = time.time() + timeout
        messages_info = [defaultdict(list) for _ in topics]
        total_counts = [0] * len(topics)
        next_report = time.time()
        processes = processes or []

        with ExitStack() as stack:
            readers = [
                stack.enter_context(self.driver.topic_client.reader(topic, consumer))
                for topic, consumer in topics
            ]

            while any(count < expected_count for count in total_counts):
                for label, process in processes:
                    returncode = process.poll()
                    if returncode is not None:
                        raise AssertionError(
                            "Kafka Streams process exited before target topics became readable: "
                            f"{label}, returncode={returncode}, target_counts={total_counts}"
                        )

                remaining = deadline - time.time()
                if remaining <= 0:
                    break

                made_progress = False
                receive_timeout = min(0.2, remaining)
                for i, reader in enumerate(readers):
                    if total_counts[i] >= expected_count:
                        continue
                    try:
                        mess = reader.receive_message(timeout=receive_timeout)
                        messages_info[i][mess.partition_id].append([mess.partition_id, mess.seqno, mess.created_at])
                        total_counts[i] += 1
                        reader.commit(mess)
                        made_progress = True
                    except TimeoutError:
                        pass

                if not made_progress and time.time() >= next_report:
                    print(f"Waiting for target messages: {total_counts}")
                    next_report = time.time() + 10

        missing = [
            f"{topic} got {total_counts[i]}"
            for i, (topic, _) in enumerate(topics)
            if total_counts[i] < expected_count
        ]
        if missing:
            raise AssertionError(
                f"Target topics did not expose {expected_count} readable messages each: "
                + "; ".join(missing)
            )

        return messages_info

    def assert_payload_indexes(
            self,
            messages_info,
            expected_prefix: bytes,
            expected_count: int,
            allow_duplicates: bool):
        indexes = []
        for messages in messages_info.values():
            for message in messages:
                indexes.append(self.extract_payload_index(message[3], expected_prefix))

        expected_indexes = set(range(expected_count))
        actual_indexes = set(indexes)
        missing_indexes = sorted(expected_indexes - actual_indexes)
        unexpected_indexes = sorted(actual_indexes - expected_indexes)
        if missing_indexes or unexpected_indexes:
            raise AssertionError(
                "Direct Kafka producer payload indexes mismatch: "
                f"missing={missing_indexes[:10]}, unexpected={unexpected_indexes[:10]}, "
                f"expected_count={expected_count}, actual_unique_count={len(actual_indexes)}"
            )

        if not allow_duplicates and len(indexes) != len(actual_indexes):
            raise AssertionError(
                "Direct Kafka transactional producer exposed duplicate payload indexes: "
                f"message_count={len(indexes)}, unique_count={len(actual_indexes)}"
            )

    def extract_payload_index(self, payload: bytes, expected_prefix: bytes):
        marker = expected_prefix + b"-message-"
        if not payload.startswith(marker):
            raise AssertionError(f"Unexpected payload prefix: {payload[:64]!r}")

        end = payload.find(b"-", len(marker))
        if end == -1:
            raise AssertionError(f"Cannot parse payload index: {payload[:64]!r}")
        return int(payload[len(marker):end])

    def count_messages(self, messages_info):
        return sum(len(messages) for messages in messages_info.values())

    def create_topic(self, topic: str, consumers: list[str]):
        try:
            self.driver.topic_client.drop_topic(topic)
        except ydb.SchemeError:
            pass

        self.driver.topic_client.create_topic(
            topic,
            consumers=consumers,
            min_active_partitions=SOURCE_TOPIC_PARTITIONS,
        )
        self.wait_topic_ready(topic, SOURCE_TOPIC_PARTITIONS)

    def wait_topic_ready(self, topic: str, min_partitions: int, timeout=30):
        deadline = time.time() + timeout
        last_count = 0
        while time.time() < deadline:
            description = self.driver.topic_client.describe_topic(topic)
            last_count = len(description.partitions)
            if last_count >= min_partitions:
                return
            time.sleep(0.2)
        raise AssertionError(
            f"{topic} did not expose {min_partitions} partitions: got {last_count}"
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver is not None:
            self.driver.stop()
        for tmp_dir in self.tmp_dirs:
            shutil.rmtree(tmp_dir)
