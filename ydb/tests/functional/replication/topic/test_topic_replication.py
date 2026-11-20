from contextlib import contextmanager, suppress
import time

import grpc

from ydb.public.api.grpc.draft import ydb_replication_v1_pb2_grpc
from ydb.public.api.protos.draft import ydb_replication_pb2
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


DATABASE = "/Root"
TOPIC = "/Root/events"
REPLICATION = "/Root/replication"
CONSUMER = "application"
REPLICATION_USER = "root@builtin"
TIMEOUT = 60


@contextmanager
def database(name):
    config = KikimrConfigGenerator(
        nodes=1,
        erasure=Erasure.NONE,
        default_clusteradmin=REPLICATION_USER,
        extra_feature_flags=["enable_topic_replication", "enable_insecure_mirror_factory"],
        extra_grpc_services=["replication"],
    )
    cluster = KiKiMR(config, cluster_name=name)
    cluster.start()
    try:
        endpoint = cluster.nodes[1].endpoint
        with ydb.Driver(
            endpoint=endpoint, database=DATABASE, credentials=ydb.AccessTokenCredentials(REPLICATION_USER),
        ) as driver:
            driver.wait(timeout=TIMEOUT)
            yield endpoint, driver
    finally:
        cluster.stop()


def wait_for(description, observe, expected):
    deadline = time.monotonic() + TIMEOUT
    actual = None
    while time.monotonic() < deadline:
        try:
            actual = observe()
        except (ydb.SchemeError, ydb.NotFound, ydb.Unavailable, ydb.Overloaded, ydb.Timeout) as error:
            actual = error
        except grpc.RpcError as error:
            if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                raise
            actual = error
        if actual == expected:
            return
        time.sleep(0.25)
    raise AssertionError(f"Timed out waiting for {description}: expected {expected!r}, got {actual!r}")


def offsets(client):
    consumer = client.describe_consumer(TOPIC, CONSUMER, include_stats=True)
    assert len(consumer.partitions) == 1, consumer
    partition = consumer.partitions[0]
    assert partition.partition_id == 0, partition
    if partition.partition_stats is None or partition.partition_consumer_stats is None:
        return None
    return partition.partition_stats.partition_end, partition.partition_consumer_stats.committed_offset


def replication_state(stub):
    operation = stub.DescribeReplication(
        ydb_replication_pb2.DescribeReplicationRequest(path=REPLICATION),
        metadata=(("x-ydb-database", DATABASE), ("x-ydb-auth-ticket", REPLICATION_USER)),
        timeout=5,
    ).operation
    assert operation.ready, operation
    ydb.issues._process_response(operation)
    result = ydb_replication_pb2.DescribeReplicationResult()
    assert operation.result.Unpack(result), operation
    state = result.WhichOneof("state")
    assert state != "error", result
    return state


def messages(prefix):
    return [f"{prefix}-{index}".encode() for index in range(128)]


def write(client, payloads):
    with client.writer(TOPIC, partition_id=0) as writer:
        try:
            writer.write_with_ack(payloads, timeout=TIMEOUT)
        except Exception:
            with suppress(Exception):
                writer.close(flush=False, timeout=TIMEOUT)
            raise


def read(client, payloads, first_offset):
    deadline = time.monotonic() + TIMEOUT
    with client.reader(TOPIC, consumer=CONSUMER) as reader:
        for index, expected in enumerate(payloads):
            remaining = deadline - time.monotonic()
            assert remaining > 0, f"Timed out reading {TOPIC} at offset {first_offset + index}"
            message = reader.receive_message(timeout=remaining)
            assert (message.partition_id, message.offset, message.data) == (0, first_offset + index, expected)


def test_topic_replication_and_failover():
    with (
        database("source") as (source_endpoint, source),
        database("destination") as (destination_endpoint, destination),
    ):
        with ydb.QuerySessionPool(source) as queries:
            queries.execute_with_retries(f"CREATE TOPIC `{TOPIC}`;")
            queries.execute_with_retries(f"ALTER TOPIC `{TOPIC}` ADD CONSUMER `{CONSUMER}`;")

        source_topic = source.topic_client
        destination_topic = destination.topic_client
        initial = messages("initial")
        write(source_topic, initial)

        with ydb.QuerySessionPool(destination) as queries, grpc.insecure_channel(destination_endpoint) as channel:
            queries.execute_with_retries(
                f'CREATE ASYNC REPLICATION `{REPLICATION}` FOR `{TOPIC}` AS `{TOPIC}` WITH ('
                f'CONNECTION_STRING = "grpc://{source_endpoint}/?database={DATABASE}", '
                f'TOKEN = "{REPLICATION_USER}", CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);'
            )
            replication = ydb_replication_v1_pb2_grpc.ReplicationServiceStub(channel)
            wait_for("replication to start", lambda: replication_state(replication), "running")

            def destination_offsets():
                replication_state(replication)
                return offsets(destination_topic)

            initial_end = len(initial)
            wait_for("initial messages on the destination", destination_offsets, (initial_end, 0))
            read(destination_topic, initial, 0)

            source_topic.commit_offset(TOPIC, CONSUMER, 0, initial_end)
            wait_for(
                "initial consumer offset on the destination", destination_offsets,
                (initial_end, initial_end),
            )

            live = messages("live")
            write(source_topic, live)
            live_end = initial_end + len(live)
            wait_for("live messages on the destination", destination_offsets, (live_end, initial_end))
            read(destination_topic, live, initial_end)
            source_topic.commit_offset(TOPIC, CONSUMER, 0, live_end)
            wait_for(
                "live consumer offset on the destination", destination_offsets, (live_end, live_end),
            )

            queries.execute_with_retries(
                f'ALTER ASYNC REPLICATION `{REPLICATION}` SET (STATE = "DONE", FAILOVER_MODE = "FORCE");'
            )
            wait_for("failover completion", lambda: replication_state(replication), "done")
            queries.execute_with_retries(f"DROP ASYNC REPLICATION `{REPLICATION}`;")

        destination_end = live_end
        for prefix in ("after-failover", "after-commit"):
            local = messages(prefix)
            write(destination_topic, local)
            read(destination_topic, local, destination_end)
            destination_end += len(local)
            destination_topic.commit_offset(TOPIC, CONSUMER, 0, destination_end)
            wait_for(
                "local consumer offset after failover", lambda: offsets(destination_topic),
                (destination_end, destination_end),
            )

        assert offsets(source_topic) == (live_end, live_end)
