"""Persisted ACK retention through actual current / released ydbd transitions."""
import asyncio
import base64
import datetime
import gzip
import json
import logging
import random
import time

import grpc
import pytest

from ydb.core.protos import msgbus_pb2 as msgbus
from ydb.public.api.grpc import ydb_topic_v1_pb2_grpc
from ydb.public.api.protos import ydb_table_pb2, ydb_topic_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.library.compatibility.fixtures import (
    RollingDowngradeAndUpgradeFixture,
    current_binary_path,
    current_name,
    inter_stable_binary_path,
    inter_stable_name,
)
from ydb.tests.oss.ydb_sdk_import import ydb

from ydb.tests.compatibility.topics.cdc_replication_common import (
    CdcReplicationCompatibilityFixture,
    CdcReplicationWorkload,
    poll,
)
from ydb import _apis
from ydb._grpc.grpcwrapper.common_utils import GrpcWrapperAsyncIO
from ydb._grpc.grpcwrapper.ydb_topic import Codec, StreamReadMessage

logger = logging.getLogger(__name__)
ACK_FLAG = 'enable_replication_topic_acknowledged_retention'
ACK_POLICY = 'SOURCE_TOPIC_RETENTION_ACKNOWLEDGED'
TEST_TTL = 30
METADATA = (('x-ydb-database', '/Root'),)


def private_topic(fixture, topic_path):
    request = msgbus.TSchemeDescribe(Path=topic_path + '/streamImpl')
    request.Options.ShowPrivateTable = True
    request.Options.ReturnPartitioningInfo = True
    request.Options.ReturnPartitionConfig = True
    response = fixture.source_cluster.client.send(request, 'SchemeDescribe')
    description = response.PathDescription
    assert description.HasField('PersQueueGroup'), response
    return description


def snapshot(workload):
    description = private_topic(workload.fixture, workload.topic_path)
    config = description.PersQueueGroup.PQTabletConfig
    part = config.PartitionConfig
    consumers = [c for c in config.Consumers if c.Name == workload.consumer_name]
    assert len(consumers) == 1, config
    consumer = consumers[0]
    public = workload.fixture.source_driver.topic_client.describe_topic(workload.topic_path, include_stats=True)
    assert len(public.partitions) == 1, public
    stats = public.partitions[0].partition_stats
    result = {
        'path_identity': (description.Self.SchemeshardId, description.Self.PathId),
        'policy': part.RetentionPolicy,
        'required': part.RetentionConsumer,
        'lifetime': part.LifetimeSeconds,
        'consumer': (consumer.Name, consumer.Version, consumer.Generation, consumer.Important,
                     consumer.ReadFromTimestampsMs, consumer.AvailabilityPeriodMs),
        'start': stats.partition_start,
        'end': stats.partition_end,
        'store_bytes': stats.store_size_bytes,
        'commit': workload.checkpoint(),
    }
    logger.info('ACK snapshot %s: %r', workload.topic_path, result)
    return result


def assert_contract(workload, original=None, *, acknowledged=True, lifetime=86400):
    state = snapshot(workload)
    assert state['lifetime'] == lifetime, state
    assert state['policy'] == int(acknowledged), state
    assert state['required'] == (workload.consumer_name if acknowledged else ''), state
    name, version, generation, important, read_from, availability = state['consumer']
    assert name == workload.consumer_name
    assert important == acknowledged, state
    assert read_from == 0, state
    if acknowledged:
        assert availability == 0, state
    assert state['commit'] <= state['end'], state
    if acknowledged:
        assert state['start'] <= state['commit'], state
    if original is not None:
        workload.assert_stream_identity()
        assert state['path_identity'] == original['path_identity'], (original, state)
        assert state['consumer'] == original['consumer'], (original, state)
        assert state['commit'] >= original['commit'], (original, state)
    return state


def alter(fixture, topic_path, *, endpoint=None, expected=StatusIds.SUCCESS, **kwargs):
    # Direct gRPC guarantees the selected old node handles the API request;
    # SDK discovery is allowed to reconnect to a current node in a mixed cluster.
    endpoint = (endpoint or fixture.source_endpoint).removeprefix('grpc://')
    request = ydb_topic_pb2.AlterTopicRequest(path=topic_path, **kwargs)
    with grpc.insecure_channel(endpoint) as channel:
        response = ydb_topic_v1_pb2_grpc.TopicServiceStub(channel).AlterTopic(
            request, timeout=60, metadata=METADATA)
    assert response.operation.ready, response
    assert response.operation.status == expected, response


def ordinary_old_alters(workload, original, endpoint=None):
    fixture = workload.fixture
    # The same-default speed request exercises an idempotent old API roundtrip.
    for kwargs in (
        {'set_partition_write_speed_bytes_per_second': 1048576},
        {'add_consumers': [ydb_topic_pb2.Consumer(name='ordinary')]},
        {'alter_consumers': [ydb_topic_pb2.AlterConsumer(name='ordinary', alter_attributes={'_version': '19'})]},
        {'drop_consumers': ['ordinary']},
    ):
        alter(fixture, workload.topic_path, endpoint=endpoint, **kwargs)
        assert_contract(workload, original)
        if 'alter_consumers' in kwargs:
            config = private_topic(fixture, workload.topic_path).PersQueueGroup.PQTabletConfig
            ordinary = [consumer for consumer in config.Consumers if consumer.Name == 'ordinary']
            assert len(ordinary) == 1 and ordinary[0].Version == 19, config
    alter(fixture, workload.topic_path, endpoint=endpoint, expected=StatusIds.ALREADY_EXISTS,
          add_consumers=[ydb_topic_pb2.Consumer(name=workload.consumer_name)])
    assert_contract(workload, original)


def current_guards(workload, original):
    for kwargs in (
        {'drop_consumers': [workload.consumer_name]},
        {'drop_consumers': [workload.consumer_name],
         'add_consumers': [ydb_topic_pb2.Consumer(name=workload.consumer_name, important=True)]},
        {'alter_consumers': [ydb_topic_pb2.AlterConsumer(name=workload.consumer_name,
                                                        alter_attributes={'_version': '19'})]},
    ):
        alter(workload.fixture, workload.topic_path, expected=StatusIds.BAD_REQUEST, **kwargs)
        assert_contract(workload, original)


def restart_source_tablets(fixture):
    for tablet_type in (TabletTypes.FLAT_SCHEMESHARD, TabletTypes.PERSQUEUE):
        response = fixture.source_cluster.client.tablet_state(tablet_type)
        ids = [entry.TabletId for entry in response.TabletStateInfo]
        assert ids, (tablet_type, response)
        for tablet_id in ids:
            fixture.source_cluster.client.tablet_kill(tablet_id)
    poll('source ready after SchemeShard/PQ restart',
         lambda: fixture.source_driver.scheme_client.list_directory('/Root'), lambda _: True)


async def read_range(driver, topic_path, consumer, begin, end):
    """Read a captured durable range without acknowledging any consumer offset."""
    stream = GrpcWrapperAsyncIO(StreamReadMessage.FromServer.from_proto)
    await stream.start(driver, _apis.TopicService.Stub, _apis.TopicService.StreamRead)
    found = {}
    deadline = time.monotonic() + 180
    try:
        stream.write(StreamReadMessage.FromClient(StreamReadMessage.InitRequest(
            topics_read_settings=[StreamReadMessage.InitRequest.TopicReadSettings(path=topic_path)],
            consumer=consumer, auto_partitioning_support=True)))
        while len(found) < end - begin and time.monotonic() < deadline:
            try:
                event = await stream.receive(timeout=5)
            except TimeoutError:
                continue
            message = event.server_message
            if isinstance(message, StreamReadMessage.StartPartitionSessionRequest):
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.StartPartitionSessionResponse(
                    partition_session_id=message.partition_session.partition_session_id,
                    read_offset=begin, commit_offset=None)))
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.ReadRequest(bytes_size=64 * 1024 * 1024)))
            elif isinstance(message, StreamReadMessage.StopPartitionSessionRequest):
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.StopPartitionSessionResponse(
                    partition_session_id=message.partition_session_id)))
            elif isinstance(message, StreamReadMessage.ReadResponse):
                for partition in message.partition_data:
                    for batch in partition.batches:
                        assert batch.codec in (Codec.CODEC_RAW, Codec.CODEC_GZIP), batch
                        for record in batch.message_data:
                            if begin <= record.offset < end:
                                body = gzip.decompress(record.data) if batch.codec == Codec.CODEC_GZIP else record.data
                                found[record.offset] = (record.offset, body, batch.written_at)
                stream.write(StreamReadMessage.FromClient(StreamReadMessage.ReadRequest(bytes_size=64 * 1024 * 1024)))
        assert sorted(found) == list(range(begin, end)), (begin, end, sorted(found))
        return [found[offset] for offset in range(begin, end)]
    finally:
        stream.close()


class RetentionWorkload(CdcReplicationWorkload):
    def __init__(self, fixture, name, consistency='ROW'):
        super().__init__(fixture, name, consistency)
        self.next_key = 1

    def batch(self, label, count=24):
        # Separate SQL transactions force several independently persisted blobs;
        # alternating >512KiB values also exercises multipart boundaries.
        before = self.topic_end()
        for index in range(count):
            key = self.next_key
            self.next_key += 1
            payload = base64.b64encode(random.Random(key).randbytes(393240)).decode('ascii') if index % 2 else ''
            value = label + ':' + str(key) + payload
            binary = b'\x00\xffA'
            self.upsert(key, value, binary=binary, optional=None)
        poll('batch reaches CDC topic', self.topic_end, lambda end: end >= before + count)

    def exact(self):
        self.wait_rows(self.expected_rows())
        end = self.topic_end()
        self.wait_checkpoint(end)
        return assert_contract(self)

    def update_delete(self, label):
        key = min(self.expected)
        self.upsert(key, label, binary=b'\x00\xffA', optional=None)
        key = max(self.expected)
        self.erase(key)

    def unread(self, captured=None):
        state = snapshot(self)
        assert state['end'] > state['commit'], state
        begin, end = state['commit'], state['end']
        if captured is not None:
            # GLOBAL keeps appending resolved heartbeats during a version switch.
            # Preserve exactly the original durable range, including its lower bound.
            begin, end = captured[0][0], captured[-1][0] + 1
            assert state['commit'] == begin, (begin, state)
            assert state['start'] <= begin < end <= state['end'], (begin, end, state)
        audit = 'audit_' + str(time.monotonic_ns())
        alter(self.fixture, self.topic_path, add_consumers=[ydb_topic_pb2.Consumer(name=audit)])
        try:
            messages = asyncio.run(read_range(
                self.fixture.source_driver, self.topic_path, audit,
                begin, end))
        finally:
            alter(self.fixture, self.topic_path, drop_consumers=[audit])
        assert [record[0] for record in messages] == list(range(begin, end))
        assert snapshot(self)['commit'] == state['commit'], 'Audit read must not acknowledge replication'
        return messages

    def reclaimed(self, old_start=0):
        # Public live range proves policy execution; C++ tests retain KV deletion checks.
        return poll('fresh acknowledged prefix reclaimed before unchanged one-day TTL',
                    lambda: snapshot(self),
                    lambda s: old_start < s['start'] <= s['commit'] and s['lifetime'] == 86400,
                    timeout=180)


class TestAcknowledgedRetentionCompatibility(CdcReplicationCompatibilityFixture):
    @pytest.fixture(autouse=True, params=[[current_binary_path, inter_stable_binary_path]],
                    ids=[f'ack_{current_name}_{inter_stable_name}'])
    def base_setup(self, request):
        if current_name != 'current':
            pytest.skip('ACK retention acceptance requires patched target: -DYDB_COMPAT_TARGET_REF=current')
        self.all_binary_paths = request.param
        self.current_binary_paths_index = 0

    def configure_binary(self, role, config, binary_path):
        super().configure_binary(role, config, binary_path)
        pq = config.yaml_config.setdefault('pqconfig', {})
        pq['valid_retention_limits'] = [{'min_period_seconds': 1, 'max_period_seconds': 604800,
                                         'min_storage_megabytes': 0, 'max_storage_megabytes': 0}]
        compaction = pq.setdefault('compaction_config', {})
        compaction['blobs_count'] = 0
        pq['max_blob_size'] = 1024 * 1024
        flags = config.yaml_config.setdefault('feature_flags', {})
        flags['enable_topic_disk_sub_domain_quota'] = True
        if role == 'source':
            if binary_path == current_binary_path:
                flags[ACK_FLAG] = True
            else:
                flags.pop(ACK_FLAG, None)
        if role == 'destination':
            defaults = config.yaml_config.setdefault('replication_config', {})
            if binary_path == current_binary_path:
                defaults['source_topic_retention_policy'] = ACK_POLICY
            else:
                defaults.pop('source_topic_retention_policy', None)

    @pytest.fixture(autouse=True)
    def setup(self):
        yield from self.setup_cdc_clusters(source_options={'separate_node_configs': True})

    @pytest.mark.parametrize('consistency', ['ROW', 'GLOBAL'])
    def test_acknowledged_retention_binary_lifecycle(self, consistency):
        # Separate missing-request/source controls on these same persistent clusters.
        self.switch_source(inter_stable_binary_path)
        source_fallback = RetentionWorkload(self, 'old_source_' + consistency, consistency)
        source_fallback.create_source_table()
        source_fallback.create_replication()
        source_fallback.discover_changefeed()
        assert_contract(source_fallback, acknowledged=False)
        source_fallback.batch('old-source', count=2)
        source_fallback.wait_rows(source_fallback.expected_rows())
        self.switch_source(current_binary_path)
        assert_contract(source_fallback, acknowledged=False)
        source_fallback.update_delete('source-upgraded')
        source_fallback.wait_rows(source_fallback.expected_rows())

        self.stop_destination()
        self.restart_destination(inter_stable_binary_path)
        destination_fallback = RetentionWorkload(self, 'old_destination_' + consistency, consistency)
        destination_fallback.create_source_table()
        destination_fallback.create_replication()
        destination_fallback.discover_changefeed()
        assert_contract(destination_fallback, acknowledged=False)
        destination_fallback.batch('old-destination', count=2)
        destination_fallback.wait_rows(destination_fallback.expected_rows())
        self.stop_destination()
        self.restart_destination(current_binary_path)
        assert_contract(destination_fallback, acknowledged=False)
        destination_fallback.update_delete('destination-upgraded')
        destination_fallback.wait_rows(destination_fallback.expected_rows())

        active = RetentionWorkload(self, 'active_' + consistency, consistency)
        active.create_source_table()
        active.create_replication()
        active.discover_changefeed()
        active.batch('current-prefix')
        original = active.exact()
        active.reclaimed()
        current_guards(active, original)

        # N source / old destination. Keep outstanding work on every transition.
        self.stop_destination()
        active.batch('destination-rollback-backlog')
        suffix = active.unread()
        self.restart_destination(inter_stable_binary_path)
        active.exact()
        active.update_delete('old-destination-update')
        active.exact()
        assert_contract(active, original)

        # Both old, retaining unread history across old PQ + old schema persistence.
        self.stop_destination()
        active.batch('source-rollback-backlog')
        suffix = active.unread()
        checkpoint = active.checkpoint()
        self.switch_source(inter_stable_binary_path)
        assert_contract(active, original)
        assert active.checkpoint() == checkpoint
        assert active.unread(captured=suffix) == suffix
        ordinary_old_alters(active, original)
        restart_source_tablets(self)
        assert_contract(active, original)
        assert active.unread(captured=suffix) == suffix
        self.restart_destination(inter_stable_binary_path)
        active.exact()
        active.update_delete('both-old-update')
        active.exact()

        # Old source / N destination, with old durable apply state recovered by N.
        self.stop_destination()
        active.batch('destination-reupgrade-backlog')
        suffix = active.unread()
        self.restart_destination(current_binary_path)
        active.exact()
        active.update_delete('old-source-current-destination')
        active.exact()

        # Return source to N without topic repair or consumer recreation.
        self.stop_destination()
        active.batch('source-reupgrade-backlog')
        suffix = active.unread()
        self.switch_source(current_binary_path)
        assert_contract(active, original)
        assert active.unread(captured=suffix) == suffix
        self.restart_destination(current_binary_path)
        active.exact()
        before = snapshot(active)['start']
        active.batch('fresh-current-reclaim')
        active.exact()
        active.reclaimed(before)
        current_guards(active, original)
        ordinary_old_alters(active, original)

    def test_acknowledged_retention_unread_survives_old_ttl(self):
        # TEST-ONLY short lifetime. The replication product default remains 86400.
        self.stop_destination()
        paths = []
        expected = {}
        for policy in ('acknowledged', 'standard'):
            workload = RetentionWorkload(self, 'ttl_' + policy)
            workload.create_source_table()
            attrs = {'id': '1', 'path': '/Root/unused'}
            if policy == 'acknowledged':
                attrs.update(retention_policy=policy, retention_consumer='required')
            request = ydb_table_pb2.AlterTableRequest(path=workload.table_path)
            feed = request.add_changefeeds.add(name='ttl', mode=ydb_table_pb2.ChangefeedMode.MODE_UPDATES,
                                              format=ydb_table_pb2.ChangefeedFormat.FORMAT_JSON)
            feed.retention_period.seconds = TEST_TTL
            feed.attributes['__async_replication'] = json.dumps(attrs)
            session = self.source_driver.table_client.session().create()
            request.session_id = session.session_id
            try:
                response = self.source_driver(
                    request, _apis.TableService.Stub, _apis.TableService.AlterTable,
                    lambda state, result: result, ydb.BaseRequestSettings().with_timeout(60),
                    (), session._state.endpoint)
                assert response.operation.status == StatusIds.SUCCESS, response
            finally:
                session.delete()
            workload.topic_path = workload.table_path + '/ttl'
            workload.consumer_name = 'required'
            if policy == 'standard':
                alter(self, workload.topic_path, add_consumers=[ydb_topic_pb2.Consumer(name='required')])
            assert_contract(workload, acknowledged=policy == 'acknowledged', lifetime=TEST_TTL)
            workload.batch('short-ttl', count=24)
            # Capture exactly all records without committing the required offset.
            expected[policy] = workload.unread()
            paths.append(workload)
        self.switch_source(inter_stable_binary_path)
        # Both topics have identical age/traffic. The ordinary topic must actually
        # expire, proving the Important topic saw a real cleanup opportunity.
        expire_after = max(record[2] for records in expected.values() for record in records) + datetime.timedelta(seconds=TEST_TTL + 3)
        while datetime.datetime.utcnow() < expire_after:
            time.sleep(min(1, (expire_after - datetime.datetime.utcnow()).total_seconds()))
        for workload in paths:
            workload.batch('cleanup-trigger', count=4)
        restart_source_tablets(self)
        poll('old STANDARD topic expires unread records', lambda: snapshot(paths[1]),
             lambda s: s['start'] > 0, timeout=180)
        state = snapshot(paths[0])
        assert state['start'] == 0 and state['commit'] == 0, state
        actual = paths[0].unread()
        assert actual[:len(expected['acknowledged'])] == expected['acknowledged']
        assert_contract(paths[0], acknowledged=True, lifetime=TEST_TTL)

    def test_acknowledged_retention_active_rolling_source(self):
        active = RetentionWorkload(self, 'rolling_active')
        active.create_source_table()
        active.create_replication()
        active.discover_changefeed()
        active.batch('before-rolling')
        original = active.exact()
        active.reclaimed()

        # Reuse the established rolling fixture against the existing source peer.
        # Its creation-only ACK startup flag can be removed once the contract is
        # durably persisted. PQ cleanup reads that persisted contract on N.
        for node in self.source_cluster.nodes.values():
            config = node.read_node_config()
            config.setdefault('feature_flags', {}).pop(ACK_FLAG, None)
            config.setdefault('replication_config', {}).pop('source_topic_retention_policy', None)
            self.source_cluster.overwrite_configs(config, node_ids=[node.node_id])
        source = RollingDowngradeAndUpgradeFixture()
        source.all_binary_paths = [current_binary_path, inter_stable_binary_path]
        source.cluster = self.source_cluster
        source.config = self.source_config
        source.database_path = '/Root'
        source.endpoints = ['grpc://' + node.endpoint for node in source.cluster.nodes.values()]
        source.driver = self.source_driver
        saw_mixed = False
        saw_all_old = False
        for stage, _ in enumerate(source.roll()):
            self.source_driver = source.driver
            binaries = [node.binary_path for node in self.source_cluster.nodes.values()]
            logger.info('ACK rolling source stage=%d binaries=%r', stage, binaries)
            active.batch('rolling-' + str(stage), count=4)
            active.exact()
            assert_contract(active, original)
            old_nodes = [node for node in self.source_cluster.nodes.values()
                         if node.binary_path == inter_stable_binary_path]
            if old_nodes and current_binary_path in binaries:
                saw_mixed = True
                ordinary_old_alters(active, original, endpoint=old_nodes[0].endpoint)
            if len(old_nodes) == len(binaries):
                saw_all_old = True
                self.audit_binary('source_rolling_all_old', self.source_cluster, inter_stable_binary_path)
                self.stop_destination()
                active.batch('all-old-unread')
                suffix = active.unread()
                restart_source_tablets(self)
                assert active.unread(captured=suffix) == suffix
                ordinary_old_alters(active, original, endpoint=old_nodes[0].endpoint)
                self.restart_destination(current_binary_path)
                active.exact()
        assert saw_mixed and saw_all_old
        self.source_driver = source.driver
        assert all(node.binary_path == current_binary_path for node in self.source_cluster.nodes.values())
        self.audit_binary('source_rolling_all_current', self.source_cluster, current_binary_path)
        before = snapshot(active)['start']
        active.batch('after-rolling')
        active.exact()
        active.reclaimed(before)
        assert_contract(active, original)
        current_guards(active, original)
