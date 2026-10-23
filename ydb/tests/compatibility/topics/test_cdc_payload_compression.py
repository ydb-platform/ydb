# -*- coding: utf-8 -*-
"""N/N-1 CDC compatibility with public, undecoded envelope evidence."""
import asyncio
from dataclasses import dataclass
import gzip
import json
import time

import pytest

from ydb import _apis
from ydb._grpc.grpcwrapper.common_utils import GrpcWrapperAsyncIO
from ydb._grpc.grpcwrapper.ydb_topic import Codec, StreamReadMessage
from ydb.tests.library.compatibility.fixtures import (
    current_binary_path, current_name, inter_stable_binary_path, inter_stable_name,
    inter_stable_version, path_to_version,
)
from ydb.tests.oss.ydb_sdk_import import ydb

from ydb.tests.compatibility.topics.cdc_replication_common import (
    CdcReplicationCompatibilityFixture, CdcReplicationWorkload, describe_table, poll, query,
)


LARGE = "Привет:" + "Q" * 16384
COLUMNS = ("key", "value", "binary", "optional")
SCHEMA_COLUMNS = COLUMNS + ("extra",)
PROBE = "compression_probe"


@dataclass(frozen=True)
class CdcWireRecord:
    partition: int
    offset: int
    producer_id: str
    write_session_meta: tuple
    codec: int
    written_at: object
    seq_no: int
    created_at: object
    message_group_id: str
    metadata_items: tuple
    uncompressed_size: int
    wire_body: bytes
    decoded_body: bytes

    @property
    def json(self):
        return json.loads(self.decoded_body)

    @property
    def key(self):
        key = self.json.get("key")
        return key[0] if key is not None else None

    def persisted_identity(self):
        # Batch grouping/written_at may change on another read. These fields
        # belong to the persisted message or its producer/codec, and must not.
        return (self.partition, self.offset, self.producer_id, self.write_session_meta,
                self.codec, self.seq_no, self.created_at, self.message_group_id,
                self.metadata_items, self.uncompressed_size, self.wire_body)


async def _capture(driver, topic_path, start, end, timeout):
    stream = GrpcWrapperAsyncIO(StreamReadMessage.FromServer.from_proto)
    await stream.start(driver, _apis.TopicService.Stub, _apis.TopicService.StreamRead)
    records = {}
    sessions = {}
    deadline = time.monotonic() + timeout
    try:
        stream.write(StreamReadMessage.FromClient(StreamReadMessage.InitRequest(
            topics_read_settings=[StreamReadMessage.InitRequest.TopicReadSettings(path=topic_path)],
            consumer=PROBE, auto_partitioning_support=True,
        )))
        while len(records) < end - start and time.monotonic() < deadline:
            try:
                message = await stream.receive(timeout=min(5, max(0.1, deadline - time.monotonic())))
            except TimeoutError:
                continue
            server = message.server_message
            if isinstance(server, StreamReadMessage.StartPartitionSessionRequest):
                session = server.partition_session
                assert session.partition_id == 0, session
                sessions[session.partition_session_id] = session.partition_id
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.StartPartitionSessionResponse(
                    partition_session_id=session.partition_session_id,
                    read_offset=max((record.offset + 1 for record in records.values()), default=start), commit_offset=None,
                )))
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.ReadRequest(bytes_size=50 * 1024 * 1024)))
            elif isinstance(server, StreamReadMessage.StopPartitionSessionRequest):
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.StopPartitionSessionResponse(
                    partition_session_id=server.partition_session_id,
                )))
                sessions.pop(server.partition_session_id, None)
            elif isinstance(server, StreamReadMessage.ReadResponse):
                for partition_data in server.partition_data:
                    partition = sessions[partition_data.partition_session_id]
                    for batch in partition_data.batches:
                        assert batch.codec in (Codec.CODEC_RAW, Codec.CODEC_GZIP), batch.codec
                        for data in batch.message_data:
                            if not start <= data.offset < end:
                                continue
                            decoded = gzip.decompress(data.data) if batch.codec == Codec.CODEC_GZIP else data.data
                            if batch.codec == Codec.CODEC_GZIP:
                                assert data.uncompresed_size == len(decoded), (data.offset, data.uncompresed_size, len(decoded))
                                assert len(data.data) < len(decoded), (data.offset, len(data.data), len(decoded))
                            record = CdcWireRecord(
                                partition, data.offset, batch.producer_id, tuple(sorted(batch.write_session_meta.items())),
                                batch.codec, batch.written_at, data.seq_no, data.created_at, data.message_group_id,
                                tuple(sorted(data.metadata_items.items())), data.uncompresed_size, data.data, decoded,
                            )
                            identity = (partition, data.offset)
                            assert identity not in records, f"Duplicate topic message {identity}"
                            records[identity] = record
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.ReadRequest(bytes_size=50 * 1024 * 1024)))
        result = sorted(records.values(), key=lambda record: (record.partition, record.offset))
        assert [record.offset for record in result] == list(range(start, end)), (start, end, result)
        return result
    finally:
        # Probe has its own consumer and deliberately sends no commits.
        stream.close()


def capture_topic_history(workload, read_offset=0, expected_data_keys=frozenset(), timeout=120, end_offset=None):
    deadline = time.monotonic() + timeout
    records = []
    while time.monotonic() < deadline:
        end = workload.topic_end() if end_offset is None else end_offset
        assert end - read_offset <= 4096, (read_offset, end)
        records = asyncio.run(_capture(workload.source_driver, workload.topic_path, read_offset, end,
                                       max(0.1, deadline - time.monotonic()))) if end > read_offset else []
        if expected_data_keys <= {record.key for record in records if record.key is not None}:
            return records
        if end_offset is not None:
            break
        time.sleep(1)
    raise AssertionError(f"Missing CDC keys {expected_data_keys}; records={records!r}")


def add_probe(workload):
    workload.source_driver.topic_client.alter_topic(workload.topic_path, add_consumers=[PROBE])
    description = workload.source_driver.topic_client.describe_topic(workload.topic_path)
    assert description.retention_period.total_seconds() == 86400, description.retention_period
    assert workload.replication_attributes.get("retention_policy") is None, workload.replication_attributes


def assert_mutation(record, key, *, value=None, optional=None, extra=None, erase=False):
    body = record.json
    assert body["key"] == [key] and type(body["key"][0]) is int, body
    if erase:
        assert body["erase"] == {} and "update" not in body, body
    else:
        expected = {"value": value, "binary": "AP9B", "optional": optional}
        if extra is not None:
            expected["extra"] = extra
        assert body["update"] == expected and "erase" not in body, body
        if optional is not None:
            assert type(body["update"]["optional"]) is int, body
        if extra is not None:
            assert type(body["update"]["extra"]) is int, body


def only_key(records, key):
    matches = [record for record in records if record.key == key]
    assert len(matches) == 1, (key, matches)
    return matches[0]


def assert_codec(record, codec):
    assert record.codec == codec, (
        f"CDC key={record.key}, offset={record.offset}: expected public codec {codec}, got {record.codec}; "
        f"wire={len(record.wire_body)}, decoded={len(record.decoded_body)}"
    )


def assert_prefix_unchanged(before, after):
    assert [record.persisted_identity() for record in after[:len(before)]] == [record.persisted_identity() for record in before]


class CdcCompressionCompatibilityFixture(CdcReplicationCompatibilityFixture):
    def configure_binary(self, role, config, binary_path):
        super().configure_binary(role, config, binary_path)
        if role == "source":
            datashard = config.yaml_config.setdefault("data_shard_config", {})
            if binary_path == current_binary_path:
                datashard["cdc_payload_compression"] = "CDC_PAYLOAD_COMPRESSION_GZIP"
            else:
                datashard.pop("cdc_payload_compression", None)

    @pytest.fixture(autouse=True)
    def setup(self, internal_decompression):
        if current_name != "current":
            pytest.skip("CDC GZIP acceptance requires patched target: -DYDB_COMPAT_TARGET_REF=current")
        if inter_stable_version < (26, 4, 1):
            pytest.skip("CDC rollback schema/decoder floor is stable-26-4-1")
        flags = ["enable_async_replication_schema_changes"] if self.schema_changes else []
        disabled = [] if internal_decompression else ["transfer_internal_data_decompression"]
        enabled = flags + (["transfer_internal_data_decompression"] if internal_decompression else [])
        yield from self.setup_cdc_clusters(
            source_options={"extra_feature_flags": flags},
            destination_options={"extra_feature_flags": enabled, "disabled_feature_flags": disabled},
        )

    def seeded(self, name, consistency="ROW"):
        workload = CdcReplicationWorkload(self, name, consistency)
        workload.create_source_table()
        workload.upsert(1, LARGE)
        workload.upsert(2, "tiny", optional=7)
        workload.create_replication()
        add_probe(workload)
        workload.wait_rows(workload.expected_rows())
        return workload


class TestCdcPayloadCompressionRestart(CdcCompressionCompatibilityFixture):
    schema_changes = False

    @pytest.fixture(autouse=True, params=[[inter_stable_binary_path, current_binary_path]],
                    ids=[f"restart_{inter_stable_name}_to_{current_name}"])
    def base_setup(self, request):
        self.current_binary_paths_index = 0
        self.all_binary_paths = request.param
        self.versions = [path_to_version[path] for path in self.all_binary_paths]

    @pytest.mark.parametrize("internal_decompression", [False, True], ids=["sdk_decoder", "internal_decoder"])
    def test_replication_codec_history_survives_source_and_destination_rollback(self, internal_decompression):
        # Preserve the predecessor's creation-time codec declaration across upgrade.
        legacy = self.seeded("legacy")
        legacy_codec = (Codec.CODEC_GZIP if "gzip" in legacy.replication_attributes.get("supported_payload_codecs", [])
                        else Codec.CODEC_RAW)
        if inter_stable_version == (26, 4, 1):
            assert "supported_payload_codecs" not in legacy.replication_attributes
        initial = capture_topic_history(legacy, expected_data_keys={1, 2})
        assert_codec(only_key(initial, 1), legacy_codec)
        assert_codec(only_key(initial, 2), Codec.CODEC_RAW)
        assert_mutation(only_key(initial, 1), 1, value=LARGE)
        assert_mutation(only_key(initial, 2), 2, value="tiny", optional=7)
        legacy.upsert(3, LARGE + "old-destination")
        legacy.wait_rows(legacy.expected_rows())
        old_live = capture_topic_history(legacy, read_offset=len(initial), expected_data_keys={3})
        assert_codec(only_key(old_live, 3), legacy_codec)

        self.change_cluster_version()  # Actual stable -> patched destination.
        legacy.assert_stream_identity()
        legacy.upsert(4, LARGE + "no-retrofit")
        legacy.wait_rows(legacy.expected_rows())
        upgraded_live = capture_topic_history(legacy, read_offset=len(initial) + len(old_live), expected_data_keys={4})
        assert_codec(only_key(upgraded_live, 4), legacy_codec)
        legacy.assert_stream_identity()

        capable = self.seeded("capable")
        assert capable.replication_attributes["supported_payload_codecs"] == ["gzip"]
        initial = capture_topic_history(capable, expected_data_keys={1, 2})
        assert_codec(only_key(initial, 1), Codec.CODEC_GZIP)
        assert_mutation(only_key(initial, 1), 1, value=LARGE)
        assert_codec(only_key(initial, 2), Codec.CODEC_RAW)
        assert_mutation(only_key(initial, 2), 2, value="tiny", optional=7)

        start = len(initial)
        capable.upsert(1, LARGE + "updated", optional=11)
        capable.wait_rows(capable.expected_rows())
        updated = capture_topic_history(capable, read_offset=start, expected_data_keys={1})
        update_record = only_key(updated, 1)
        assert_codec(update_record, Codec.CODEC_GZIP)
        assert_mutation(update_record, 1, value=LARGE + "updated", optional=11)
        start += len(updated)
        capable.upsert(3, "small", optional=5)
        capable.wait_rows(capable.expected_rows())
        tiny = capture_topic_history(capable, read_offset=start, expected_data_keys={3})
        assert_codec(only_key(tiny, 3), Codec.CODEC_RAW)
        assert_mutation(only_key(tiny, 3), 3, value="small", optional=5)
        start += len(tiny)
        capable.erase(2)
        capable.wait_rows(capable.expected_rows())
        erased = capture_topic_history(capable, read_offset=start, expected_data_keys={2})
        assert_codec(only_key(erased, 2), Codec.CODEC_RAW)
        assert_mutation(only_key(erased, 2), 2, erase=True)

        self.stop_destination()  # Every destination process is stopped before new writes.
        committed_before = capable.checkpoint()
        capable.upsert(4, LARGE + "unread-gzip")
        before = capture_topic_history(capable, expected_data_keys={4})
        pending_gzip = only_key(before, 4)
        assert_codec(pending_gzip, Codec.CODEC_GZIP)
        assert_mutation(pending_gzip, 4, value=LARGE + "unread-gzip")
        assert committed_before <= capable.checkpoint() <= pending_gzip.offset

        self.switch_source(inter_stable_binary_path)
        capable.assert_stream_identity()
        legacy.assert_stream_identity()
        capable.upsert(5, LARGE + "old-source-raw")
        after = capture_topic_history(capable, expected_data_keys={5})
        assert_prefix_unchanged(before, after)
        pending_raw = only_key(after, 5)
        assert_codec(pending_raw, Codec.CODEC_RAW)
        assert_mutation(pending_raw, 5, value=LARGE + "old-source-raw")
        assert committed_before <= capable.checkpoint() <= pending_gzip.offset
        sequences = [record.seq_no for record in after]
        assert all(left < right for left, right in zip(sequences, sequences[1:])), sequences

        self.change_cluster_version()  # Actual patched -> stable on existing disks.
        capable.wait_rows(capable.expected_rows())
        capable.wait_checkpoint(max(pending_gzip.offset, pending_raw.offset) + 1)
        legacy.wait_rows(legacy.expected_rows())
        self.restart_destination()  # Second old restart proves persisted progress.
        capable.upsert(6, LARGE + "old-resumed")
        resumed = capture_topic_history(capable, read_offset=len(after), expected_data_keys={6})
        assert_codec(only_key(resumed, 6), Codec.CODEC_RAW)
        assert_mutation(only_key(resumed, 6), 6, value=LARGE + "old-resumed")
        capable.wait_rows(capable.expected_rows())
        capable.wait_checkpoint(only_key(resumed, 6).offset + 1)

        self.switch_source(current_binary_path)
        capable.assert_stream_identity()
        capable.upsert(7, LARGE + "new-source-old-destination")
        final = capture_topic_history(capable, expected_data_keys={7})
        assert_prefix_unchanged(after, final)
        assert_codec(only_key(final, 7), Codec.CODEC_GZIP)
        assert_mutation(only_key(final, 7), 7, value=LARGE + "new-source-old-destination")
        assert [record.key for record in final[len(initial):]] == [1, 3, 2, 4, 5, 6, 7]
        capable.wait_rows(capable.expected_rows())
        capable.wait_checkpoint(only_key(final, 7).offset + 1)
        legacy.assert_stream_identity()

        self.change_cluster_version()  # Destination re-upgrade resumes the same stream.
        capable.assert_stream_identity()
        capable.upsert(8, LARGE + "destination-reupgraded")
        reupgraded = capture_topic_history(capable, read_offset=len(final), expected_data_keys={8})
        assert_codec(only_key(reupgraded, 8), Codec.CODEC_GZIP)
        assert_mutation(only_key(reupgraded, 8), 8, value=LARGE + "destination-reupgraded")
        capable.wait_rows(capable.expected_rows())
        capable.wait_checkpoint(only_key(reupgraded, 8).offset + 1)


class TestCdcPayloadCompressionSchemaRollback(CdcCompressionCompatibilityFixture):
    schema_changes = True

    @pytest.fixture(autouse=True, params=[[current_binary_path, inter_stable_binary_path]],
                    ids=[f"restart_{current_name}_to_{inter_stable_name}"])
    def base_setup(self, request):
        self.current_binary_paths_index = 0
        self.all_binary_paths = request.param
        self.versions = [path_to_version[path] for path in self.all_binary_paths]

    @pytest.mark.parametrize("internal_decompression", [True], ids=["internal_decoder"])
    def test_current_schema_barrier_and_gzip_resume_on_previous_release(self, internal_decompression):
        workload = self.seeded("schema", consistency="GLOBAL")
        assert workload.replication_attributes["supported_payload_codecs"] == ["gzip"]
        initial = capture_topic_history(workload, expected_data_keys={1, 2})
        assert_codec(only_key(initial, 1), Codec.CODEC_GZIP)
        assert_mutation(only_key(initial, 1), 1, value=LARGE)
        assert_codec(only_key(initial, 2), Codec.CODEC_RAW)

        # Apply schema while destination is current. The preceding release has
        # an unrelated fresh-ALTER oneof bug; current's persisted schema must
        # remain readable, replicated and resumable through old restarts.
        query(self.source_driver, f"ALTER TABLE `{workload.table_path}` ADD COLUMN extra Uint32;")
        workload.upsert(3, LARGE + "current-applied-schema", extra=17)
        poll("replicated extra column", lambda: describe_table(self.driver, workload.replica_path),
             lambda table: "extra" in {column.name for column in table.columns})
        workload.wait_rows(workload.expected_rows(SCHEMA_COLUMNS), SCHEMA_COLUMNS)

        def barrier_history():
            records = capture_topic_history(workload, expected_data_keys={3})
            data = only_key(records, 3)
            schemas = [record for record in records if "tableChanges" in record.json]
            resolved = [record for record in records if "resolved" in record.json]
            if not schemas or not any(record.offset > data.offset and tuple(record.json["resolved"]) >= tuple(data.json["ts"])
                                      for record in resolved):
                return None
            return records
        before = poll("current schema barrier followed by GLOBAL resolved timestamp", barrier_history)
        schema_records = [record for record in before if "tableChanges" in record.json]
        data = only_key(before, 3)
        assert_codec(data, Codec.CODEC_GZIP)
        assert_mutation(data, 3, value=LARGE + "current-applied-schema", extra=17)
        assert len(data.json["ts"]) == 2 and all(type(value) is int for value in data.json["ts"]), data.json
        assert "extra" in schema_records[-1].json["tableChanges"][0]["table"]["columns"]
        assert schema_records[-1].offset < data.offset
        for record in schema_records:
            assert_codec(record, Codec.CODEC_RAW)
            assert len(record.json["ts"]) == 2 and all(type(value) is int for value in record.json["ts"]), record.json
        for record in before:
            if "resolved" in record.json:
                assert_codec(record, Codec.CODEC_RAW)
                assert len(record.json["resolved"]) == 2 and all(type(value) is int for value in record.json["resolved"]), record.json

        self.stop_destination()
        checkpoint = workload.checkpoint()
        workload.upsert(4, LARGE + "pending-after-barrier", extra=42)
        pending_history = capture_topic_history(workload, expected_data_keys={4})
        pending = only_key(pending_history, 4)
        assert_codec(pending, Codec.CODEC_GZIP)
        assert_mutation(pending, 4, value=LARGE + "pending-after-barrier", extra=42)
        assert checkpoint <= workload.checkpoint() <= pending.offset
        assert_prefix_unchanged(before, pending_history)

        self.change_cluster_version()
        workload.wait_rows(workload.expected_rows(SCHEMA_COLUMNS), SCHEMA_COLUMNS)
        workload.wait_checkpoint(pending.offset + 1)
        assert "extra" in {column.name for column in describe_table(self.driver, workload.replica_path).columns}
        reread = capture_topic_history(workload, end_offset=pending_history[-1].offset + 1)
        assert_prefix_unchanged(pending_history, reread)

        self.restart_destination()
        workload.upsert(5, LARGE + "old-restarted-global", extra=53)
        resumed = capture_topic_history(workload, expected_data_keys={5})
        assert_codec(only_key(resumed, 5), Codec.CODEC_GZIP)
        assert_mutation(only_key(resumed, 5), 5, value=LARGE + "old-restarted-global", extra=53)
        workload.wait_rows(workload.expected_rows(SCHEMA_COLUMNS), SCHEMA_COLUMNS)
        workload.wait_checkpoint(only_key(resumed, 5).offset + 1)
        with pytest.raises(ydb.BadRequest) as rejected:
            query(self.driver, f"UPSERT INTO `{workload.replica_path}` (key,value) VALUES (999u,'forbidden');")
        pending_issues = list(rejected.value.issues)
        messages = []
        while pending_issues:
            issue = pending_issues.pop()
            messages.append(issue.message)
            pending_issues.extend(issue.issues)
        assert any(message.startswith("Can't execute write tx at replicated table:") for message in messages), messages
        workload.wait_rows(workload.expected_rows(SCHEMA_COLUMNS), SCHEMA_COLUMNS)
        workload.assert_stream_identity()
