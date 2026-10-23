# -*- coding: utf-8 -*-
"""Actual-binary, independent source/destination CDC compatibility helpers."""
import hashlib
import json
import logging
import os
import time

import yatest.common

from ydb import _apis, issues
from ydb._grpc.grpcwrapper.common_utils import ydb_topic_pb2
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    current_binary_path,
    current_name,
    inter_stable_binary_path,
    inter_stable_name,
    prepare_feature_flags,
    prepare_table_service_config,
)
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)
_OMITTED = object()


def poll(description, fn, predicate=bool, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = fn()
            if predicate(last):
                return last
        except ydb.Error as error:
            last = repr(error)
        time.sleep(1)
    raise AssertionError(f"Timed out waiting for {description}; last observation: {last!r}")


def query(driver, sql, parameters=None):
    settings = ydb.BaseRequestSettings().with_timeout(30).with_operation_timeout(30)
    with ydb.QuerySessionPool(driver) as pool:
        return pool.execute_with_retries(sql, parameters=parameters, settings=settings)


def _operation_result(result_type):
    def unwrap(rpc_state, response):
        issues._process_response(response.operation)
        result = result_type()
        assert response.operation.result.Unpack(result)
        return result
    return unwrap


def describe_table(driver, path):
    # The Python TableSchemeEntry wrapper drops changefeeds. Preserve the public
    # protobuf and route this request to the node owning the SDK-created session.
    session = driver.table_client.session().create()
    try:
        request = _apis.ydb_table.DescribeTableRequest(session_id=session.session_id, path=path)
        return driver(
            request, _apis.TableService.Stub, _apis.TableService.DescribeTable,
            _operation_result(_apis.ydb_table.DescribeTableResult),
            ydb.BaseRequestSettings().with_timeout(30), (), session._state.endpoint,
        )
    finally:
        session.delete()


def describe_topic_proto(driver, path):
    return driver(
        ydb_topic_pb2.DescribeTopicRequest(path=path, include_stats=True),
        _apis.TopicService.Stub, _apis.TopicService.DescribeTopic,
        _operation_result(ydb_topic_pb2.DescribeTopicResult),
        ydb.BaseRequestSettings().with_timeout(30),
    )


class CdcReplicationCompatibilityFixture(RestartToAnotherVersionFixture):
    """A persistent source in addition to the normal restart-fixture destination."""

    def configure_binary(self, role, config, binary_path):
        """Feature-owned hook. Remove unknown startup options before old startup."""

    def _config_for(self, role, binary_path, options):
        options = dict(options or {})
        enabled, disabled = prepare_feature_flags(
            options.pop("extra_feature_flags", []), options.pop("disabled_feature_flags", []),
        )
        config = KikimrConfigGenerator(
            nodes=3, erasure=Erasure.MIRROR_3_DC, use_in_memory_pdisks=False,
            binary_paths=[binary_path], extra_feature_flags=enabled,
            disabled_feature_flags=disabled,
            table_service_config=prepare_table_service_config(options.pop("table_service_config", {})),
            output_path=yatest.common.test_output_path(f"cdc_{role}"), **options,
        )
        self.configure_binary(role, config, binary_path)
        return config

    @staticmethod
    def _driver(endpoint):
        driver = ydb.Driver(ydb.DriverConfig(database="/Root", endpoint=endpoint))
        driver.wait(timeout=60)
        return driver

    def setup_cdc_clusters(self, source_binary_path=current_binary_path,
                           source_options=None, destination_options=None):
        self.binary_evidence = []
        self.binary_hashes = {}
        self.driver = self.source_driver = None
        self.cluster = self.source_cluster = None
        self.database_path = "/Root"
        self.source_binary_path = source_binary_path
        destination_path = self.all_binary_paths[self.current_binary_paths_index]
        self.config = self._config_for("destination", destination_path, destination_options)
        self.source_config = self._config_for("source", source_binary_path, source_options)
        try:
            self.cluster = KiKiMR(self.config)
            self.cluster.start()
            self.endpoint = f"grpc://localhost:{self.cluster.nodes[1].port}"
            self.driver = self.create_driver()
            self.audit_binary("destination", self.cluster, destination_path)
            self.source_cluster = KiKiMR(self.source_config, cluster_name="cdc_source")
            self.source_cluster.start()
            self.source_endpoint = f"grpc://localhost:{self.source_cluster.nodes[1].port}"
            self.source_driver = self._driver(self.source_endpoint)
            self.audit_binary("source", self.source_cluster, source_binary_path)
            yield
        finally:
            self.stop_driver()
            self.stop_source_driver()
            try:
                if self.source_cluster is not None:
                    self.source_cluster.stop()
            finally:
                if self.cluster is not None:
                    self.cluster.stop()

    def audit_binary(self, role, cluster, path):
        if path not in self.binary_hashes:
            digest = hashlib.sha256()
            with open(path, "rb") as binary:
                for chunk in iter(lambda: binary.read(8 * 1024 * 1024), b""):
                    digest.update(chunk)
            self.binary_hashes[path] = digest.hexdigest()
        name = {current_binary_path: current_name, inter_stable_binary_path: inter_stable_name}.get(path, path)
        for node_id, node in cluster.nodes.items():
            assert os.path.samefile(node.binary_path, path), (role, node_id, node.binary_path, path)
            executable = f"/proc/{node.pid}/exe"
            if os.path.exists(executable):
                assert os.path.samefile(executable, path), (role, node_id, os.readlink(executable), path)
            evidence = dict(role=role, node_id=node_id, pid=node.pid, path=path, name=name,
                            sha256=self.binary_hashes[path], version=node.get_node_binary_version())
            logger.info("CDC compatibility binary: %s", evidence)
            self.binary_evidence.append(evidence)
        with open(yatest.common.test_output_path("cdc_binary_versions.json"), "w") as output:
            json.dump(self.binary_evidence, output, indent=2)

    def stop_source_driver(self):
        if self.source_driver is not None:
            self.source_driver.stop()
            self.source_driver = None

    def switch_source(self, binary_path):
        self.stop_source_driver()
        self.configure_binary("source", self.source_config, binary_path)
        self.source_config.set_binary_paths([binary_path])
        self.source_cluster.update_configurator_and_restart(self.source_config)
        self.source_binary_path = binary_path
        self.source_endpoint = f"grpc://localhost:{self.source_cluster.nodes[1].port}"
        self.source_driver = self._driver(self.source_endpoint)
        self.audit_binary("source", self.source_cluster, binary_path)
        poll("source schema service after restart", lambda: query(self.source_driver, "SELECT 1;"))

    def stop_destination(self):
        self.stop_driver()
        self.cluster.stop()

    def restart_destination(self, binary_path=None):
        binary_path = binary_path or self.all_binary_paths[self.current_binary_paths_index]
        self.current_binary_paths_index = self.all_binary_paths.index(binary_path)
        self.stop_driver()
        self.configure_binary("destination", self.config, binary_path)
        self.config.set_binary_paths([binary_path])
        self.cluster.update_configurator_and_restart(self.config)
        self.endpoint = f"grpc://localhost:{self.cluster.nodes[1].port}"
        self.driver = self.create_driver()
        self.audit_binary("destination", self.cluster, binary_path)
        poll("destination schema service after restart", lambda: query(self.driver, "SELECT 1;"))

    def change_cluster_version(self):
        next_path = self.all_binary_paths[(self.current_binary_paths_index + 1) % len(self.all_binary_paths)]
        self.configure_binary("destination", self.config, next_path)
        super().change_cluster_version()
        self.audit_binary("destination", self.cluster, next_path)


class CdcReplicationWorkload:
    def __init__(self, fixture, name, consistency="ROW"):
        self.fixture = fixture
        self.table_path = f"/Root/{name}_source"
        self.replica_path = f"/Root/{name}_replica"
        self.replication_path = f"/Root/{name}_replication"
        self.consistency = consistency
        self.expected = {}

    @property
    def source_driver(self):
        return self.fixture.source_driver

    def create_source_table(self):
        query(self.source_driver, f"""
            CREATE TABLE `{self.table_path}` (
                `key` Uint32 NOT NULL, `value` Utf8, `binary` String, `optional` Uint32, PRIMARY KEY(`key`)
            ) WITH (UNIFORM_PARTITIONS=1, AUTO_PARTITIONING_BY_SIZE=DISABLED,
                    AUTO_PARTITIONING_BY_LOAD=DISABLED);
        """)

    def upsert(self, key, value, binary=b"\x00\xffA", optional=None, extra=_OMITTED):
        declarations = "DECLARE $key AS Uint32; DECLARE $value AS Utf8; DECLARE $binary AS String; DECLARE $optional AS Uint32?;"
        columns = "`key`,`value`,`binary`,`optional`"
        values = "$key,$value,$binary,$optional"
        parameters = {"$key": ydb.TypedValue(key, ydb.PrimitiveType.Uint32),
                      "$value": ydb.TypedValue(value, ydb.PrimitiveType.Utf8),
                      "$binary": ydb.TypedValue(binary, ydb.PrimitiveType.String),
                      "$optional": ydb.TypedValue(optional, ydb.OptionalType(ydb.PrimitiveType.Uint32))}
        row = dict(key=key, value=value, binary=binary, optional=optional)
        if extra is not _OMITTED:
            declarations += " DECLARE $extra AS Uint32?;"
            columns += ",`extra`"
            values += ",$extra"
            parameters["$extra"] = ydb.TypedValue(extra, ydb.OptionalType(ydb.PrimitiveType.Uint32))
            row["extra"] = extra
        query(self.source_driver, f"{declarations} UPSERT INTO `{self.table_path}` ({columns}) VALUES ({values});", parameters)
        self.expected[key] = row

    def erase(self, key):
        query(self.source_driver, f"DECLARE $key AS Uint32; DELETE FROM `{self.table_path}` WHERE key=$key;",
              {"$key": ydb.TypedValue(key, ydb.PrimitiveType.Uint32)})
        self.expected.pop(key)

    def create_replication(self):
        global_settings = ", CONSISTENCY_LEVEL='GLOBAL', COMMIT_INTERVAL=Interval('PT1S')" if self.consistency == "GLOBAL" else ""
        query(self.fixture.driver, f"""
            CREATE ASYNC REPLICATION `{self.replication_path}`
            FOR `{self.table_path}` AS `{self.replica_path}`
            WITH (CONNECTION_STRING='{self.fixture.source_endpoint}/?database=/Root'{global_settings});
        """)
        self.discover_changefeed()

    def discover_changefeed(self):
        def find():
            matches = []
            for feed in describe_table(self.source_driver, self.table_path).changefeeds:
                encoded = feed.attributes.get("__async_replication")
                if encoded and json.loads(encoded).get("path") == self.replica_path:
                    matches.append(feed)
            assert len(matches) <= 1, matches
            return matches[0] if matches else None
        feed = poll(f"replication changefeed for {self.table_path}", find)
        self.feed_name = feed.name
        self.feed_attributes = dict(feed.attributes)
        self.replication_attributes = json.loads(feed.attributes["__async_replication"])
        self.topic_path = f"{self.table_path}/{self.feed_name}"
        description = poll("source topic", lambda: self.source_driver.topic_client.describe_topic(self.topic_path),
                           lambda topic: bool(topic.consumers))
        declared = self.replication_attributes.get("retention_consumer")
        names = [consumer.name for consumer in description.consumers]
        if declared is not None:
            assert declared in names, (declared, names)
            self.consumer_name = declared
        else:
            assert len(names) == 1, names
            self.consumer_name = names[0]
        self.stream_identity = self._stream_identity()
        return feed

    def _stream_identity(self):
        feeds = [feed for feed in describe_table(self.source_driver, self.table_path).changefeeds if feed.name == self.feed_name]
        assert len(feeds) == 1, feeds
        topic = describe_topic_proto(self.source_driver, self.topic_path)
        # created_at is immutable public schema identity, unlike changing size/stats.
        return (self.topic_path, dict(feeds[0].attributes), topic.self.created_at.SerializeToString())

    def assert_stream_identity(self):
        assert self._stream_identity() == self.stream_identity

    def expected_rows(self, columns=("key", "value", "binary", "optional")):
        return [tuple(self.expected[key].get(column) for column in columns) for key in sorted(self.expected)]

    def wait_rows(self, expected, columns=("key", "value", "binary", "optional")):
        def rows():
            selected = ",".join(f"`{column}`" for column in columns)
            result = query(self.fixture.driver, f"SELECT {selected} FROM `{self.replica_path}` ORDER BY key;")
            return [tuple(row[column] for column in columns) for row in result[0].rows]
        return poll(f"exact replica rows at {self.replica_path}", rows, lambda rows: rows == expected)

    def topic_end(self):
        topic = self.source_driver.topic_client.describe_topic(self.topic_path, include_stats=True)
        assert len(topic.partitions) == 1, topic.partitions
        return topic.partitions[0].partition_stats.partition_end

    def checkpoint(self, consumer=None):
        description = self.source_driver.topic_client.describe_consumer(
            self.topic_path, consumer or self.consumer_name, include_stats=True,
        )
        assert len(description.partitions) == 1, description.partitions
        stats = description.partitions[0].partition_consumer_stats
        assert stats is not None, description
        return stats.committed_offset

    def wait_checkpoint(self, offset):
        return poll(f"{self.consumer_name} checkpoint >= {offset}", self.checkpoint, lambda value: value >= offset)
