"""KIKIMR-27431: stop GLOBAL replication after destination disk rollback."""

from contextlib import ExitStack, contextmanager
from pathlib import Path
import time

import grpc
import yatest.common

from ydb.public.api.grpc import ydb_topic_v1_pb2_grpc
from ydb.public.api.grpc.draft import ydb_replication_v1_pb2_grpc
from ydb.public.api.protos import ydb_topic_pb2
from ydb.public.api.protos.draft import ydb_replication_pb2
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


DATABASE = "/Root"
METADATA = (("x-ydb-database", DATABASE),)
TIMEOUT = 60


def wait_for(description, predicate, allow_missing_table=False):
    deadline = time.monotonic() + TIMEOUT
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except ydb.Error as error:
            if not isinstance(error, (
                ydb.Unavailable, ydb.Overloaded, ydb.Timeout,
                ydb.ConnectionFailure, ydb.ConnectionLost, ydb.DeadlineExceed,
            )):
                if not (allow_missing_table and isinstance(error, ydb.SchemeError)):
                    raise
            last_error = error
        except grpc.RpcError as error:
            if error.code() not in (
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                grpc.StatusCode.DEADLINE_EXCEEDED,
            ):
                raise
            last_error = error
        time.sleep(0.25)
    raise AssertionError(f"Timed out waiting for {description}; last error: {last_error}")


def unpack(response, result_type):
    operation = response.operation
    assert operation.ready, operation
    ydb.issues._process_response(operation)
    result = result_type()
    assert operation.result.Unpack(result), operation
    return result


class Database:
    def __init__(self, name):
        directory = Path(yatest.common.output_path(name))
        directory.mkdir()
        self.cluster = KiKiMR(KikimrConfigGenerator(
            nodes=1,
            erasure=Erasure.NONE,
            output_path=str(directory),
            pdisk_store_path=str(directory),
            static_pdisk_size=16 * 1024**3,
            use_in_memory_pdisks=False,
            extra_grpc_services=["replication"],
        ), cluster_name=name)
        self.disk_paths = [Path(disk["pdisk_path"]) for disk in self.cluster.config.pdisks_info]
        for path in self.disk_paths:
            assert path.parent.resolve() == directory.resolve(), "Test requires its own file-backed PDisks"
        self.driver = None
        self.pool = None
        self.channel = None

    def connect(self):
        endpoint = self.cluster.nodes[1].endpoint
        self.driver = ydb.Driver(endpoint=endpoint, database=DATABASE)
        self.driver.wait(timeout=30)
        self.pool = ydb.QuerySessionPool(self.driver)
        self.channel = grpc.insecure_channel(endpoint)
        wait_for("query service after reconnect", lambda: self.query("SELECT 1;"))

    def disconnect(self):
        if self.pool is not None:
            self.pool.stop(timeout=10)
            self.pool = None
        if self.driver is not None:
            self.driver.stop(timeout=10)
            self.driver = None
        if self.channel is not None:
            self.channel.close()
            self.channel = None

    def stop_nodes(self):
        self.disconnect()
        for node in self.cluster.nodes.values():
            node.stop()

    def start_nodes(self):
        # KiKiMR.start() formats the disks. Restart the existing node objects.
        for node in self.cluster.nodes.values():
            node.start()
        self.connect()

    def query(self, query):
        return self.pool.execute_with_retries(
            query,
            retry_settings=ydb.RetrySettings(max_retries=0, max_session_acquire_timeout=5),
            settings=ydb.BaseRequestSettings().with_timeout(5),
        )

    def rows(self):
        return [row["id"] for row in self.query("SELECT id FROM t ORDER BY id;")[0].rows]

    def replication(self):
        response = ydb_replication_v1_pb2_grpc.ReplicationServiceStub(self.channel).DescribeReplication(
            ydb_replication_pb2.DescribeReplicationRequest(path="/Root/r", include_stats=True),
            metadata=METADATA, timeout=5,
        )
        return unpack(response, ydb_replication_pb2.DescribeReplicationResult)

    def consumer(self, topic):
        response = ydb_topic_v1_pb2_grpc.TopicServiceStub(self.channel).DescribeConsumer(
            ydb_topic_pb2.DescribeConsumerRequest(
                path=topic, consumer="replicationConsumer", include_stats=True,
            ),
            metadata=METADATA, timeout=5,
        )
        result = unpack(response, ydb_topic_pb2.DescribeConsumerResult)
        assert len(result.partitions) == 1, result
        partition = result.partitions[0]
        return (
            partition.partition_consumer_stats.committed_offset,
            partition.partition_stats.partition_offsets.end,
        )


@contextmanager
def database(name):
    instance = Database(name)
    try:
        instance.cluster.start()
        instance.connect()
        yield instance
    finally:
        try:
            instance.disconnect()
        finally:
            instance.cluster.stop()
            for path in instance.disk_paths:
                path.unlink(missing_ok=True)


def copy_disk(source, destination):
    # Preserve holes in the 16 GiB test PDisk; the actual data is only a few MiB.
    zeros = bytes(4 * 1024**2)
    with source.open("rb") as reader, destination.open("wb") as writer:
        while data := reader.read(len(zeros)):
            if data == zeros[:len(data)]:
                writer.seek(len(data), 1)
            else:
                writer.write(data)
        writer.truncate(reader.tell())


def test_destination_disk_rollback_stops_replication():
    with ExitStack() as cleanup:
        source = cleanup.enter_context(database("source"))
        destination = cleanup.enter_context(database("destination"))
        source.query("CREATE TABLE t (id Uint64 NOT NULL, value Utf8, PRIMARY KEY(id));")
        source.query('UPSERT INTO t (id, value) VALUES (1u, "initial");')
        destination.query(
            'CREATE ASYNC REPLICATION r FOR t AS t WITH ('
            f'CONNECTION_STRING="grpc://{source.cluster.nodes[1].endpoint}/?database=/Root", '
            'CONSISTENCY_LEVEL="GLOBAL");'
        )
        wait_for("initial scan", lambda: destination.rows() == [1], allow_missing_table=True)
        replication = wait_for("replication description", destination.replication)
        assert replication.WhichOneof("state") == "running", replication
        topic = "/Root/t/" + replication.items[0].source_changefeed_name

        # Source acknowledgement proves the initial destination processed source offset is durable.
        _, initial_end = source.consumer(topic)
        assert initial_end > 0
        wait_for("initial source acknowledgement", lambda: source.consumer(topic)[0] >= initial_end)

        destination.stop_nodes()
        snapshot_offset, _ = wait_for("consumer at snapshot", lambda: source.consumer(topic))
        snapshots = []
        for index, path in enumerate(destination.disk_paths):
            snapshot = Path(yatest.common.output_path(f"snapshot-{index}.dat"))
            cleanup.callback(snapshot.unlink, missing_ok=True)
            copy_disk(path, snapshot)
            snapshots.append((path, snapshot))
        destination.start_nodes()
        wait_for("first ordinary restart", lambda: destination.rows() == [1])

        # Pause/resume must retain the processed offset.
        destination.query('ALTER ASYNC REPLICATION r SET (STATE="Paused");')
        wait_for("ordinary pause", lambda: destination.replication().WhichOneof("state") == "paused")
        destination.query('ALTER ASYNC REPLICATION r SET (STATE="StandBy");')
        wait_for("ordinary resume", lambda: destination.replication().WhichOneof("state") == "running")

        batch = list(range(2, 32))
        source.query("UPSERT INTO t (id, value) VALUES " + ",".join(
            f'({key}u, "after-snapshot")' for key in batch
        ) + ";")
        delivered = [1] + batch
        wait_for("all 30 changes to be visible", lambda: destination.rows() == delivered)
        _, end_offset = wait_for("consumer after delivery", lambda: source.consumer(topic))
        wait_for("source consumer acknowledgement", lambda: source.consumer(topic)[0] >= end_offset)

        # Control: a restart without restoring old storage preserves delivered data.
        destination.stop_nodes()
        destination.start_nodes()
        wait_for("ordinary restart preserving all delivered rows", lambda: destination.rows() == delivered)
        assert wait_for("replication after ordinary restart", destination.replication).WhichOneof("state") == "running"

        destination.stop_nodes()
        acknowledged_offset, _ = wait_for("consumer before rollback", lambda: source.consumer(topic))
        assert acknowledged_offset > snapshot_offset
        for path, snapshot in snapshots:
            copy_disk(snapshot, path)
        destination.start_nodes()
        assert wait_for("consumer after rollback", lambda: source.consumer(topic))[0] >= acknowledged_offset
        source.query('UPSERT INTO t (id, value) VALUES (32u, "after-return");')

        def stopped():
            result = destination.replication()
            return result if result.WhichOneof("state") == "error" else None

        error = wait_for("rollback detection", stopped)
        assert "Source consumer committed next offset" in str(error), error
        assert "destination processed source offset" in str(error), error
        stopped_offset, _ = source.consumer(topic)

        expected = delivered + [32]
        actual = destination.rows()
        assert source.rows() == expected
        assert actual == [1]
        assert actual != expected

        # The error and the source-consumer fence must survive another restart.
        destination.stop_nodes()
        destination.start_nodes()
        wait_for("durable replication error after restart", stopped)
        assert destination.rows() == actual
        assert source.consumer(topic)[0] == stopped_offset

        # State changes must preserve the rollback error.
        for state in ("Paused", "StandBy"):
            destination.query(f'ALTER ASYNC REPLICATION r SET (STATE="{state}");')
            result = wait_for(f"replication after {state}", destination.replication)
            assert result.WhichOneof("state") == "error", result
        assert destination.rows() == actual
        assert source.consumer(topic)[0] == stopped_offset
