from collections import defaultdict
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


SOURCE_SECONDS = 10
SOURCE_MESSAGE_RATE = 100
SOURCE_MESSAGE_COUNT = SOURCE_SECONDS * SOURCE_MESSAGE_RATE

KAFKA_SOURCE_PRODUCER_JAVA = textwrap.dedent("""
    import java.nio.charset.StandardCharsets;
    import java.time.Duration;
    import java.util.Properties;
    import java.util.concurrent.atomic.AtomicReference;

    import org.apache.kafka.clients.producer.Callback;
    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerConfig;
    import org.apache.kafka.clients.producer.ProducerRecord;
    import org.apache.kafka.clients.producer.RecordMetadata;
    import org.apache.kafka.common.serialization.ByteArraySerializer;
    import org.apache.kafka.common.serialization.StringSerializer;

    public class KafkaSourceProducer {
        public static void main(String[] args) throws Exception {
            String bootstrap = args[0];
            String topic = args[1];
            int seconds = Integer.parseInt(args[2]);
            int messageRate = Integer.parseInt(args[3]);
            int messageSize = Integer.parseInt(args[4]);
            int batchSize = Integer.parseInt(args[5]);
            int lingerMs = Integer.parseInt(args[6]);
            String compressionType = args[7];

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
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");

            AtomicReference<Exception> error = new AtomicReference<>();
            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        error.compareAndSet(null, exception);
                    }
                }
            };

            byte[] value = makePayload(messageSize);
            long periodNanos = 1_000_000_000L / Math.max(1, messageRate);
            long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
            long nextSend = System.nanoTime();
            int sent = 0;

            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
                while (System.nanoTime() < deadline) {
                    Exception exception = error.get();
                    if (exception != null) {
                        throw exception;
                    }

                    producer.send(new ProducerRecord<>(topic, "key-" + (sent % 16), value), callback);
                    sent++;
                    nextSend += periodNanos;
                    long sleepNanos = nextSend - System.nanoTime();
                    if (sleepNanos > 0) {
                        Thread.sleep(sleepNanos / 1_000_000L, (int)(sleepNanos % 1_000_000L));
                    }
                }

                producer.flush();
            }

            Exception exception = error.get();
            if (exception != null) {
                throw exception;
            }
            System.out.println("KafkaSourceProducer sent " + sent + " messages");
        }

        private static byte[] makePayload(int messageSize) {
            StringBuilder builder = new StringBuilder();
            while (builder.length() < messageSize) {
                builder.append("kafka-source-batch-message-");
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
        self.create_topic(
            self.test_topic_path,
            [workloadConsumerName, checkerConsumer] + [
                f"{checkerConsumer}-{i}" for i in range(len(testOptions))
            ],
        )

        processes = []
        print("NumWorkers: ", self.num_workers)
        print("Bootstrap:", self.bootstrap, "Endpoint:", self.endpoint, "Database:", self.database)

        for i, parameters in enumerate(testOptions):
            use_transactions, use_idempotence = parameters
            targetTopicName = f"{self.target_topic_path}-{i}"
            self.create_topic(targetTopicName, [checkerConsumer, f"{checkerConsumer}-{i}"])
            for j in range(self.num_workers):
                processes.append(subprocess.Popen([
                    java_path,
                    "-jar",
                    jar_file_path,
                    self.bootstrap,
                    f"streams-store-{i * self.num_workers + j}",
                    self.test_topic_path,
                    targetTopicName,
                    f"workload-consumer-{i}",
                    use_transactions,
                    use_idempotence,
                ], start_new_session=True))

        print("Waiting for Kafka Streams startup")
        time.sleep(10)

        source_process = self.start_source_writer(java_path, jar_file_path)
        source_process.wait()
        assert source_process.returncode == 0

        print("-----------------")
        expected_source_count = SOURCE_MESSAGE_COUNT if self.source_writer == "kafka" else None
        messages_info_test = self.read_messages(
            self.test_topic_path,
            checkerConsumer,
            expected_count=expected_source_count,
            timeout=60,
        )
        source_count = self.count_messages(messages_info_test)
        print(f"Source topic has {source_count} readable messages")
        print(f"Waiting up to {self.duration} sec for readable target topic messages")
        deadline = time.time() + self.duration
        messages_info_targets = []
        for i in range(len(testOptions)):
            remaining = max(1, deadline - time.time())
            messages_info_targets.append(
                self.read_messages(
                    f"{self.target_topic_path}-{i}",
                    f"{checkerConsumer}-{i}",
                    expected_count=source_count,
                    timeout=remaining,
                )
            )

        print("Killing processes")
        for process in processes:
            self._kill_process_tree(process)

        for process in processes:
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                print(f"Process {process.pid} did not terminate in time")

        topic_description = self.driver.topic_client.describe_topic(self.test_topic_path, include_stats=True)
        print(topic_description)

        for i in range(len(testOptions)):
            messages_info_target = messages_info_targets[i]
            totalMessCountTest = self.count_messages(messages_info_test)
            totalMessCountTarget = self.count_messages(messages_info_target)

            print(f"target {self.target_topic_path}-{i}. totalMessCountTest = {totalMessCountTest}, "
                  f"totalMessCountTarget = {totalMessCountTarget}")
            if i >= 1:
                assert totalMessCountTest <= totalMessCountTarget, (
                    f"Source message count is greater than the target {self.target_topic_path}-{i} topic's "
                    f"message count: {totalMessCountTest} and {totalMessCountTarget} respectively."
                )
            else:
                assert totalMessCountTest == totalMessCountTarget, (
                    f"Source and target {self.target_topic_path}-{i} topics total messages count are not "
                    f"equal: {totalMessCountTest} and {totalMessCountTarget} respectively."
                )
            print(f"Total num of messages: {totalMessCountTest}")
        return

    def start_source_writer(self, java_path, jar_file_path):
        if self.source_writer == "kafka":
            return self.start_kafka_source_writer(java_path, jar_file_path)
        if self.source_writer == "topic":
            return self.start_topic_source_writer()
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
        print("Running Kafka producer source writer")
        producer_class_dir = "./kafka-source-producer"
        os.makedirs(producer_class_dir, exist_ok=True)
        producer_source = os.path.join(producer_class_dir, "KafkaSourceProducer.java")
        with open(producer_source, "w") as out:
            out.write(KAFKA_SOURCE_PRODUCER_JAVA)

        javac_path = os.path.join(os.path.dirname(java_path), "javac")
        subprocess.run([
            javac_path,
            "-cp",
            jar_file_path,
            producer_source,
        ], check=True, text=True)

        bootstrap = self.bootstrap
        for prefix in ("http://", "https://"):
            if bootstrap.startswith(prefix):
                bootstrap = bootstrap[len(prefix):]
                break
        target_batch_messages = 5
        linger_ms = max(1, 1000 * target_batch_messages // SOURCE_MESSAGE_RATE)
        producer_command = [
            java_path,
            "-cp",
            f"{jar_file_path}:{producer_class_dir}",
            "KafkaSourceProducer",
            bootstrap,
            self.test_topic_path,
            str(SOURCE_SECONDS),
            str(SOURCE_MESSAGE_RATE),
            "256",
            "32768",
            str(linger_ms),
            "none",
        ]
        print("Kafka producer command:", producer_command)
        return subprocess.Popen(producer_command, start_new_session=True)

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

    def read_messages(self, topic: str, consumer: str, expected_count=None, timeout=1):
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
                    messages_info[mess.partition_id].append([mess.partition_id, mess.seqno, mess.created_at])
                    total_count += 1
                    reader.commit(mess)
                except TimeoutError:
                    if expected_count is None:
                        print("Have no new messages in a second")
                        return messages_info
            if expected_count is not None and total_count < expected_count:
                raise AssertionError(f"{topic} did not expose {expected_count} readable messages: got {total_count}")
            return messages_info

    def count_messages(self, messages_info):
        return sum(len(messages) for messages in messages_info.values())

    def create_topic(self, topic: str, consumers: list[str]):
        try:
            self.driver.topic_client.drop_topic(topic)
        except ydb.SchemeError:
            pass

        self.driver.topic_client.create_topic(topic, consumers=consumers, min_active_partitions=10)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver is not None:
            self.driver.stop()
        for tmp_dir in self.tmp_dirs:
            shutil.rmtree(tmp_dir)
