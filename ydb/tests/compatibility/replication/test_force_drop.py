"""Replication data and source ownership across the standard full rolling fixture."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time

import grpc
import pytest
import yatest.common
from google.protobuf.json_format import MessageToDict
from ydb.core.protos import msgbus_pb2
from ydb.public.api.protos import ydb_topic_pb2
from ydb.public.api.protos.draft import ydb_replication_pb2
from ydb.tests.library.compatibility.fixtures import (
    RollingDowngradeAndUpgradeFixture,
    current_binary_path,
    current_binary_version,
    current_name,
    inter_stable_binary_path,
    inter_stable_name,
    string_version_to_tuple,
)
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


PREVIOUS_LABEL = os.environ.get('YDB_ROLLBACK_PREVIOUS_LABEL', inter_stable_name)
PDISK_BYTES = 8 * 1024**3


def wait_for(description, predicate, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (ydb.Error, grpc.RpcError, AssertionError) as error:
            last = str(error)
        time.sleep(0.2)
    raise AssertionError(f'Timeout waiting for {description}: {last}')


@contextmanager
def session(client):
    value = client.table_client.session().create()
    try:
        yield value
    finally:
        value.delete()


def scheme(client, query):
    with session(client) as value:
        value.execute_scheme(query, settings=ydb.BaseRequestSettings().with_timeout(60))


def data(client, query):
    with session(client) as value:
        result = value.transaction().execute(
            query, commit_tx=True, settings=ydb.BaseRequestSettings().with_timeout(30))
        return [[(row.key, row.value) for row in item.rows] for item in result]


def describe_path(cluster, path):
    request = msgbus_pb2.TSchemeDescribe(Path=path, SecurityToken='root@builtin')
    request.Options.ShowPrivateTable = True
    return cluster.client.send(request, 'SchemeDescribe')


def path_id(cluster, path):
    response = describe_path(cluster, path)
    assert response.HasField('PathDescription'), str(response)
    entry = response.PathDescription.Self
    assert entry.SchemeshardId and entry.PathId, str(response)
    return [entry.SchemeshardId, entry.PathId]


def rpc_describe(cluster, database, path, service, request_type, response_type, result_type, **kwargs):
    with grpc.insecure_channel(f'localhost:{cluster.nodes[1].grpc_port}') as channel:
        call = channel.unary_unary(
            service, request_serializer=request_type.SerializeToString,
            response_deserializer=response_type.FromString)
        response = call(request_type(path=path, **kwargs),
                        metadata=(('x-ydb-database', database),), timeout=10)
        assert response.operation.status == 400000, str(response)
        result = result_type()
        assert response.operation.result.Unpack(result), str(response)
        return result


def replication_description(cluster, database, path):
    return rpc_describe(
        cluster, database, path, '/Ydb.Replication.V1.ReplicationService/DescribeReplication',
        ydb_replication_pb2.DescribeReplicationRequest,
        ydb_replication_pb2.DescribeReplicationResponse,
        ydb_replication_pb2.DescribeReplicationResult, include_stats=True)


def source_config(cluster, stream):
    result = rpc_describe(
        cluster, '/Root', stream, '/Ydb.Topic.V1.TopicService/DescribeTopic',
        ydb_topic_pb2.DescribeTopicRequest, ydb_topic_pb2.DescribeTopicResponse,
        ydb_topic_pb2.DescribeTopicResult)
    return {'id': path_id(cluster, stream),
            'consumers': [MessageToDict(value, preserving_proto_field_name=True) for value in result.consumers],
            'retention_period': MessageToDict(result.retention_period),
            'attributes': dict(result.attributes)}


def binary_info(path, label):
    with open(path, 'rb') as handle:
        digest = hashlib.file_digest(handle, 'sha256').hexdigest()
    return {'label': label, 'path': os.path.realpath(path), 'sha256': digest,
            'version': subprocess.check_output([path, '-V'], text=True).split('\n\n', 1)[0]}


class ReplicationRollback(RollingDowngradeAndUpgradeFixture):
    @pytest.fixture(autouse=True, params=[[current_binary_path, inter_stable_binary_path]],
                    ids=[f'current_to_{PREVIOUS_LABEL}_to_current'])
    def base_setup(self, request):
        # FORCE is unshipped in the default prestable target. Start only with
        # the local current producer, rather than silently testing old/old.
        if current_name != 'current':
            pytest.skip('FORCE rollback requires YDB_COMPAT_TARGET_REF=current')
        self.all_binary_paths = request.param
        if os.path.realpath(self.all_binary_paths[0]) == os.path.realpath(self.all_binary_paths[1]):
            pytest.skip('A real previous binary is required; current/current is not rollback coverage')
        assert PREVIOUS_LABEL != 'current', 'Label runtime previous binary overrides with YDB_ROLLBACK_PREVIOUS_LABEL'
        self.versions = [current_binary_version, string_version_to_tuple(PREVIOUS_LABEL)]

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, request):
        monkeypatch.delenv('YDB_USE_IN_MEMORY_PDISKS', raising=False)
        monkeypatch.delenv('YDB_PDISKS_DIRECTORY', raising=False)
        self.output = Path(yatest.common.output_path(f'{self.__class__.__name__}-{request.node.name}'))
        self.output.mkdir(parents=True, exist_ok=True)
        self.evidence = {'binaries': [binary_info(self.all_binary_paths[0], current_name),
                                      binary_info(self.all_binary_paths[1], PREVIOUS_LABEL)],
                         'fixture': 'RollingDowngradeAndUpgradeFixture', 'cuts': [], 'steps': [],
                         'standard_framework_suppresses_compatibility_and_version_checks': True}
        source_output = self.output / 'source'
        self.source_disks = source_output / 'disks'
        self.destination_disks = self.output / 'destination' / 'disks'
        self.source_disks.mkdir(parents=True, exist_ok=True)
        self.destination_disks.mkdir(parents=True, exist_ok=True)
        self.source = KiKiMR(KikimrConfigGenerator(
            nodes=1, binary_paths=[self.all_binary_paths[0]], output_path=str(source_output),
            pdisk_store_path=str(self.source_disks), static_pdisk_size=PDISK_BYTES,
            use_in_memory_pdisks=False), cluster_name='source')
        self.source_driver = None
        self.driver = None
        self.cluster = None
        try:
            self.source.start()
            self.source_driver = self.create_source_driver()
            for _ in self.setup_cluster(
                    tenant_db='replication_rollback', nodes=3,
                    output_path=str(self.output / 'destination'),
                    pdisk_store_path=str(self.destination_disks), static_pdisk_size=PDISK_BYTES,
                    use_in_memory_pdisks=False, extra_grpc_services=['replication']):
                self.disk_identity = {str(path): [path.stat().st_ino, path.stat().st_size]
                                      for directory in (self.source_disks, self.destination_disks)
                                      for path in directory.glob('pdisk*.data')}
                assert len(self.disk_identity) == 10, self.disk_identity
                assert all(value[1] == PDISK_BYTES for value in self.disk_identity.values())
                self.evidence['persistent_disks'] = self.disk_identity
                self.evidence['framework_policy'] = {
                    'suppress_compatibility_check': self.cluster.nodes[1].read_node_config()[
                        'feature_flags']['suppress_compatibility_check'],
                    'suppress_version_check': self.config.suppress_version_check,
                }
                assert all(self.evidence['framework_policy'].values())
                try:
                    yield
                finally:
                    self.stop_driver()
        finally:
            self.stop_driver()
            if self.source_driver is not None:
                self.source_driver.stop()
            if self.cluster is not None:
                self.cluster.stop()
            self.source.stop()
            (self.output / 'evidence.json').write_text(json.dumps(self.evidence, indent=2))
            # Do not let YA expand sparse disks during result accumulation.
            # All fixture processes have stopped; only this test's disks go.
            for directory in (self.source_disks, self.destination_disks):
                for path in directory.glob('pdisk*.data'):
                    path.unlink()

    def create_source_driver(self):
        result = ydb.Driver(ydb.DriverConfig(
            endpoint=f'localhost:{self.source.nodes[1].grpc_port}', database='/Root'))
        result.wait(timeout=60, fail_fast=True)
        return result

    def path(self, name):
        return f'{self.database_path}/{name}'

    def rows(self):
        return data(self.driver, f'SELECT key, value FROM `{self.path("replica")}` ORDER BY key;')[0]

    def prepare_replication(self):
        scheme(self.source_driver, 'CREATE TABLE `/Root/original` (key Uint32, value Utf8, PRIMARY KEY (key));')
        data(self.source_driver, 'UPSERT INTO `/Root/original` (key, value) VALUES (7, "replicated"), (9, "before-rollback");')
        scheme(self.driver, f'CREATE SECRET `{self.path("source_credential")}` WITH (value="root@builtin");')
        scheme(self.driver, f'''CREATE ASYNC REPLICATION `{self.path("replication")}`
            FOR `/Root/original` AS `{self.path("replica")}` WITH (
                CONNECTION_STRING="grpc://localhost:{self.source.nodes[1].grpc_port}/?database=/Root",
                TOKEN_SECRET_PATH="{self.path("source_credential")}");''')
        wait_for('replicated rows', lambda: self.rows() == [(7, 'replicated'), (9, 'before-rollback')])
        description = replication_description(self.cluster, self.database_path, self.path('replication'))
        assert description.WhichOneof('state') == 'running', str(description)
        assert len(description.items) == 1, str(description)
        self.stream = '/Root/original/' + description.items[0].source_changefeed_name
        self.source_before = source_config(self.source, self.stream)
        assert self.source_before['consumers'], self.source_before
        self.source_table_id = path_id(self.source, '/Root/original')
        self.destination_id = path_id(self.cluster, self.path('replica'))
        self.replication_id = path_id(self.cluster, self.path('replication'))
        self.evidence.update(source_before=self.source_before, destination_id=self.destination_id,
                             replication_id=self.replication_id, source_table_id=self.source_table_id)

    def record_cut(self, step):
        nodes = [{'role': role, 'node_id': index, 'binary': os.path.realpath(node.binary_path),
                  'pid': node.daemon.process.pid}
                 for role, group in (('static', self.cluster.nodes), ('tenant', self.cluster.slots))
                 for index, node in group.items()]
        self.evidence['cuts'].append({'step': step, 'nodes': nodes})
        for filename, identity in self.disk_identity.items():
            value = Path(filename).stat()
            assert [value.st_ino, value.st_size] == identity

    def assert_full_roll(self):
        node_count = len(self.cluster.nodes) + len(self.cluster.slots)
        cuts = self.evidence['cuts']
        assert len(cuts) == 2 * node_count + 1, cuts
        assert all(node['binary'] == os.path.realpath(self.all_binary_paths[1])
                   for node in cuts[node_count]['nodes']), cuts[node_count]
        assert all(node['binary'] == os.path.realpath(self.all_binary_paths[0])
                   for node in cuts[-1]['nodes']), cuts[-1]
        self.evidence['steps'].append('all static and tenant processes rolled to previous and back to current')


class TestCompletedForceDropRollback(ReplicationRollback):
    @pytest.mark.parametrize('cascade', [False, True], ids=['retain', 'cascade'])
    def test_completed_force(self, cascade):
        self.prepare_replication()
        self.source_driver.stop()
        self.source_driver = None
        self.source.nodes[1].stop()
        with socket.socket() as probe:
            probe.settimeout(1)
            assert probe.connect_ex(('localhost', self.source.nodes[1].grpc_port)) != 0
        scheme(self.driver, f'DROP SECRET `{self.path("source_credential")}`;')
        scheme(self.driver, f'ALTER ASYNC REPLICATION `{self.path("replication")}` SET (STATE="DONE", FAILOVER_MODE="FORCE");')
        wait_for('actual DONE without source', lambda: replication_description(
            self.cluster, self.database_path, self.path('replication')).WhichOneof('state') == 'done')
        scheme(self.driver, f'DROP ASYNC REPLICATION `{self.path("replication")}` '
               + ('CASCADE ' if cascade else '') + 'FORCE;')
        self.evidence['steps'].append('FORCE completed with source process stopped and valid credential deleted')
        self.source.nodes[1].start()
        self.source_driver = self.create_source_driver()
        for step, _ in enumerate(self.roll()):
            self.record_cut(step)
            names = [entry.name for entry in self.driver.scheme_client.list_directory(self.database_path).children]
            assert 'replication' not in names
            if cascade:
                assert 'replica' not in names
            else:
                assert path_id(self.cluster, self.path('replica')) == self.destination_id
                expected = 'before-rollback' if step == 0 else f'cut-{step - 1}'
                assert self.rows() == [(7, 'replicated'), (9, expected)]
                data(self.driver, f'UPSERT INTO `{self.path("replica")}` (key, value) VALUES (9, "cut-{step}");')
                assert self.rows() == [(7, 'replicated'), (9, f'cut-{step}')]
            assert source_config(self.source, self.stream) == self.source_before
            assert path_id(self.source, '/Root/original') == self.source_table_id
            assert data(self.source_driver, 'SELECT key, value FROM `/Root/original` ORDER BY key;')[0] == [
                (7, 'replicated'), (9, 'before-rollback')]
        self.assert_full_roll()


class TestOrdinaryReplicationRollback(ReplicationRollback):
    def test_active_replication_and_drop(self):
        self.prepare_replication()
        with pytest.raises(ydb.Error, match='DONE'):
            scheme(self.driver, f'DROP ASYNC REPLICATION `{self.path("replication")}` FORCE;')
        assert path_id(self.cluster, self.path('replication')) == self.replication_id
        assert source_config(self.source, self.stream) == self.source_before
        for step, _ in enumerate(self.roll()):
            self.record_cut(step)
            data(self.source_driver, f'UPSERT INTO `/Root/original` (key, value) VALUES (9, "active-{step}");')
            wait_for('active replication after version cut',
                     lambda: self.rows() == [(7, 'replicated'), (9, f'active-{step}')])
            assert path_id(self.cluster, self.path('replica')) == self.destination_id
            assert path_id(self.cluster, self.path('replication')) == self.replication_id
        self.assert_full_roll()
        scheme(self.driver, f'DROP ASYNC REPLICATION `{self.path("replication")}`;')
        original = describe_path(self.source, '/Root/original')
        assert path_id(self.source, '/Root/original') == self.source_table_id
        assert len(original.PathDescription.Table.CdcStreams) == 0, str(original)
        assert path_id(self.cluster, self.path('replica')) == self.destination_id
        data(self.driver, f'UPSERT INTO `{self.path("replica")}` (key, value) VALUES (9, "ordinary-writable");')
        assert self.rows() == [(7, 'replicated'), (9, 'ordinary-writable')]
        self.evidence['steps'].append('ordinary DROP cleaned source and retained writable destination')
