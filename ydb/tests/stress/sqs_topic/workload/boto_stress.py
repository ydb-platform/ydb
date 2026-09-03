import logging
import threading
import time
import uuid

import boto3
import ydb
from botocore.config import Config

SQS_REGION = "ru-central1"
SECURITY_TOKEN = "root@builtin"
DEFAULT_CONSUMER = "ydb-sqs-consumer"
DEFAULT_WORKERS = 20
DEFAULT_DURATION_SECONDS = 5 * 60
DESCRIBE_INTERVAL_SECONDS = 1
COMMITTED_STALL_TIMEOUT_SECONDS = 10
DRAIN_TIMEOUT_SECONDS = 120
BOTO_CONFIG = Config(
    connect_timeout=5,
    read_timeout=20,
    retries={"max_attempts": 3, "mode": "standard"},
)


def make_sqs_topic_queue_name(topic_name, consumer=DEFAULT_CONSUMER):
    if consumer == DEFAULT_CONSUMER:
        return topic_name
    return f"{topic_name}@{consumer}"


def get_written_messages_count(driver, topic_name):
    describe = driver.topic_client.describe_topic(topic_name, include_stats=True)
    return sum(p.partition_stats.partition_end for p in describe.partitions)


def get_committed_messages_count(driver, topic_name, consumer=DEFAULT_CONSUMER):
    describe = driver.topic_client.describe_consumer(topic_name, consumer, include_stats=True)
    return sum(
        p.partition_consumer_stats.committed_offset
        for p in describe.partitions
        if p.partition_consumer_stats is not None
    )


class BotoStressWorkload:
    def __init__(
        self,
        endpoint,
        database,
        duration,
        sqs_endpoint,
        workers=DEFAULT_WORKERS,
        consumer=DEFAULT_CONSUMER,
    ):
        self.endpoint = endpoint
        self.database = database
        self.duration = duration
        self.sqs_endpoint = sqs_endpoint
        self.workers = workers
        self.consumer = consumer
        self.topic_name = f"boto_stress_{uuid.uuid4().hex}"
        self.queue_name = make_sqs_topic_queue_name(self.topic_name, consumer)
        self._queue_url = None
        self._driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self._driver.wait(timeout=60)

    def _make_boto_client(self):
        session = boto3.session.Session()
        return session.client(
            service_name="sqs",
            aws_access_key_id="unused",
            aws_secret_access_key="unused",
            aws_session_token=SECURITY_TOKEN,
            endpoint_url=self.sqs_endpoint,
            region_name=SQS_REGION,
            config=BOTO_CONFIG,
        )

    def create_topic(self):
        with ydb.QuerySessionPool(self._driver) as session_pool:
            session_pool.execute_with_retries(f"""
                CREATE TOPIC `{self.topic_name}`
                  (CONSUMER `{self.consumer}`
                    WITH (
                      type = 'shared',
                      keep_messages_order = false,
                      default_processing_timeout = Interval('PT30S')
                    )
                  );
            """)

    def drop_topic(self):
        with ydb.QuerySessionPool(self._driver) as session_pool:
            try:
                session_pool.execute_with_retries(f"DROP TOPIC `{self.topic_name}`;")
            except Exception as error:
                logging.error("Failed to drop topic %s: %r", self.topic_name, error)

    def resolve_queue_url(self):
        client = self._make_boto_client()
        last_error = None
        for attempt in range(10):
            try:
                self._queue_url = client.get_queue_url(QueueName=self.queue_name)["QueueUrl"]
                return self._queue_url
            except Exception as error:
                last_error = error
                logging.warning("get_queue_url attempt %s failed: %r", attempt + 1, error)
                time.sleep(1)
                client = self._make_boto_client()
        raise AssertionError(f"get_queue_url failed for {self.queue_name}: {last_error!r}")

    @property
    def queue_url(self):
        if self._queue_url is None:
            return self.resolve_queue_url()
        return self._queue_url

    def run(self):
        self.create_topic()
        queue_url = self.resolve_queue_url()
        stop_event = threading.Event()
        drain_event = threading.Event()
        lock = threading.Lock()
        stats = {
            "sent": 0,
            "deleted": 0,
            "send_errors": 0,
            "receive_errors": 0,
            "delete_errors": 0,
            "describe_checks": 0,
        }
        committed_history = []
        errors = []

        def record_error(where, error):
            logging.exception("%s failed", where)
            with lock:
                errors.append(f"{where}: {error!r}")

        def writer_loop(worker_id):
            client = self._make_boto_client()
            seq = 0
            while not stop_event.is_set():
                try:
                    client.send_message(
                        QueueUrl=queue_url,
                        MessageBody=f"stress-{worker_id}-{seq}",
                    )
                    with lock:
                        stats["sent"] += 1
                    seq += 1
                except Exception as error:
                    with lock:
                        stats["send_errors"] += 1
                    record_error(f"writer-{worker_id}", error)
                    client = self._make_boto_client()
                    time.sleep(0.2)

        def reader_loop(worker_id):
            client = self._make_boto_client()
            while True:
                with lock:
                    if stop_event.is_set() and stats["deleted"] >= stats["sent"]:
                        return
                try:
                    response = client.receive_message(
                        QueueUrl=queue_url,
                        WaitTimeSeconds=1,
                        MaxNumberOfMessages=10,
                    )
                    messages = response.get("Messages", [])
                    if not messages:
                        continue

                    entries = []
                    for index, message in enumerate(messages):
                        entries.append({
                            "Id": str(index),
                            "ReceiptHandle": message["ReceiptHandle"],
                        })
                    delete_response = client.delete_message_batch(
                        QueueUrl=queue_url,
                        Entries=entries,
                    )
                    if delete_response.get("Failed"):
                        with lock:
                            stats["delete_errors"] += len(delete_response["Failed"])
                        record_error(
                            f"reader-{worker_id}",
                            delete_response["Failed"],
                        )
                        continue

                    with lock:
                        stats["deleted"] += len(delete_response.get("Successful", []))
                except Exception as error:
                    with lock:
                        stats["receive_errors"] += 1
                    record_error(f"reader-{worker_id}", error)
                    client = self._make_boto_client()
                    time.sleep(0.2)

        def monitor_loop():
            monitor_driver = ydb.Driver(ydb.DriverConfig(self.endpoint, self.database))
            monitor_driver.wait(timeout=60)
            last_committed = -1
            last_progress_at = time.time()
            try:
                while not drain_event.is_set():
                    written = get_written_messages_count(monitor_driver, self.topic_name)
                    committed = get_committed_messages_count(
                        monitor_driver,
                        self.topic_name,
                        self.consumer,
                    )
                    with lock:
                        stats["describe_checks"] += 1
                        committed_history.append((time.time(), written, committed, stats["sent"], stats["deleted"]))

                    if committed < last_committed:
                        raise AssertionError(
                            f"committed offset moved backwards: {last_committed} -> {committed}"
                        )
                    if committed > last_committed:
                        last_committed = committed
                        last_progress_at = time.time()
                    elif written > committed and time.time() - last_progress_at > COMMITTED_STALL_TIMEOUT_SECONDS:
                        with lock:
                            snapshot = dict(stats)
                        raise AssertionError(
                            "committed offset stalled while messages are still in the topic: "
                            f"written={written} committed={committed} stats={snapshot}"
                        )
                    time.sleep(DESCRIBE_INTERVAL_SECONDS)
            finally:
                monitor_driver.stop()

        writer_threads = [
            threading.Thread(target=writer_loop, args=(worker_id,), name=f"sqs-writer-{worker_id}", daemon=True)
            for worker_id in range(self.workers)
        ]
        reader_threads = [
            threading.Thread(target=reader_loop, args=(worker_id,), name=f"sqs-reader-{worker_id}", daemon=True)
            for worker_id in range(self.workers)
        ]
        monitor_thread = threading.Thread(target=monitor_loop, name="sqs-monitor", daemon=True)

        logging.info(
            "Starting boto SQS stress for %s seconds: topic=%s workers=%s endpoint=%s",
            self.duration,
            self.topic_name,
            self.workers,
            self.sqs_endpoint,
        )

        for thread in writer_threads + reader_threads:
            thread.start()
        monitor_thread.start()

        time.sleep(self.duration)
        stop_event.set()

        for thread in writer_threads:
            thread.join(timeout=30)

        drain_deadline = time.time() + DRAIN_TIMEOUT_SECONDS
        while time.time() < drain_deadline:
            with lock:
                if stats["deleted"] >= stats["sent"] and stats["sent"] > 0:
                    break
            time.sleep(0.5)
        drain_event.set()

        for thread in reader_threads:
            thread.join(timeout=30)
        monitor_thread.join(timeout=10)

        committed_deadline = time.time() + 30
        committed = 0
        while time.time() < committed_deadline:
            committed = get_committed_messages_count(self._driver, self.topic_name, self.consumer)
            with lock:
                sent = stats["sent"]
            if committed >= sent:
                break
            time.sleep(1)

        written = get_written_messages_count(self._driver, self.topic_name)

        with lock:
            final_stats = dict(stats)
            final_errors = list(errors)
            history_tail = committed_history[-5:]

        logging.info(
            "Boto SQS stress finished: written=%s committed=%s stats=%s history_tail=%s",
            written,
            committed,
            final_stats,
            history_tail,
        )

        alive = [
            thread.name
            for thread in writer_threads + reader_threads + [monitor_thread]
            if thread.is_alive()
        ]
        assert not alive, f"threads are still alive: {alive}"
        assert final_stats["sent"] > 0, f"no messages were sent: {final_stats}"
        assert final_stats["deleted"] > 0, f"no messages were deleted: {final_stats}"
        assert final_stats["describe_checks"] > 0, f"topic was not described: {final_stats}"
        assert final_stats["send_errors"] < final_stats["sent"], f"too many send errors: {final_stats}"
        assert final_stats["receive_errors"] < final_stats["deleted"] + 100, (
            f"too many receive errors: {final_stats}"
        )
        assert final_stats["delete_errors"] < final_stats["deleted"], (
            f"too many delete errors: {final_stats}"
        )
        assert final_stats["deleted"] == final_stats["sent"], (
            f"not all sent messages were read and committed: {final_stats}"
        )
        assert committed >= final_stats["sent"], (
            f"committed offset lagged behind sent messages: committed={committed} sent={final_stats['sent']}"
        )
        assert written >= final_stats["sent"], (
            f"topic end offset lagged behind sent messages: written={written} sent={final_stats['sent']}"
        )
        assert not final_errors, f"worker errors: {final_errors[:10]}"

    def close(self):
        self.drop_topic()
        self._driver.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
