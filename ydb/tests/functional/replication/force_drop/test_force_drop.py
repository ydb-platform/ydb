"""Real source outage, deleted credential and persistent destination restart."""
from contextlib import contextmanager
import hashlib
import json
import os
import socket
import time

import grpc
import pytest
import yatest.common
import ydb
from google.protobuf.json_format import MessageToDict
from ydb.core.protos import msgbus_pb2
from ydb.public.api.protos import ydb_topic_pb2
from ydb.public.api.protos.draft import ydb_replication_pb2
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


def wait_for(description, predicate, timeout=90):
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
    raise AssertionError(f"Timeout waiting for {description}: {last}")


def driver(cluster):
    endpoint = f"localhost:{cluster.nodes[1].grpc_port}"
    result = ydb.Driver(ydb.DriverConfig(endpoint=endpoint, database='/Root'))
    result.wait(timeout=30, fail_fast=True)
    return result


@contextmanager
def session(client):
    value = client.table_client.session().create()
    try:
        yield value
    finally:
        value.delete()


def rows(client):
    with session(client) as table_session:
        result = table_session.transaction().execute(
            'SELECT key, value FROM `/Root/replica` ORDER BY key;', commit_tx=True)
        return [(row.key, row.value) for row in result[0].rows]


def scheme(client, query):
    with session(client) as table_session:
        table_session.execute_scheme(query)


def data(client, query):
    with session(client) as table_session:
        table_session.transaction().execute(query, commit_tx=True)


def path_id(cluster, path):
    request = msgbus_pb2.TSchemeDescribe(Path=path, SecurityToken='root@builtin')
    request.Options.ShowPrivateTable = True  # CDC stream paths are private schema objects.
    description = cluster.client.send(request, 'SchemeDescribe')
    assert description.HasField('PathDescription'), str(description)
    entry = description.PathDescription.Self
    assert entry.PathId and entry.SchemeshardId, str(description)
    return [entry.SchemeshardId, entry.PathId]


def describe_replication(cluster):
    with grpc.insecure_channel(f'localhost:{cluster.nodes[1].grpc_port}') as channel:
        call = channel.unary_unary(
            '/Ydb.Replication.V1.ReplicationService/DescribeReplication',
            request_serializer=ydb_replication_pb2.DescribeReplicationRequest.SerializeToString,
            response_deserializer=ydb_replication_pb2.DescribeReplicationResponse.FromString)
        response = call(ydb_replication_pb2.DescribeReplicationRequest(path='/Root/replication', include_stats=True),
                        metadata=(('x-ydb-database', '/Root'),), timeout=10)
        assert response.operation.status == 400000, str(response)
        result = ydb_replication_pb2.DescribeReplicationResult()
        assert response.operation.result.Unpack(result), str(response)
        return result


def source_config(cluster, stream):
    with grpc.insecure_channel(f'localhost:{cluster.nodes[1].grpc_port}') as channel:
        call = channel.unary_unary(
            '/Ydb.Topic.V1.TopicService/DescribeTopic',
            request_serializer=ydb_topic_pb2.DescribeTopicRequest.SerializeToString,
            response_deserializer=ydb_topic_pb2.DescribeTopicResponse.FromString)
        response = call(ydb_topic_pb2.DescribeTopicRequest(path=stream),
                        metadata=(('x-ydb-database', '/Root'),), timeout=10)
        assert response.operation.status == 400000, str(response)
        result = ydb_topic_pb2.DescribeTopicResult()
        assert response.operation.result.Unpack(result), str(response)
        return {'id': path_id(cluster, stream),
                'consumers': [MessageToDict(value, preserving_proto_field_name=True) for value in result.consumers],
                'retention_period': MessageToDict(result.retention_period),
                'attributes': dict(result.attributes)}


def make_cluster(name, base, binary, force):
    output = os.path.join(base, name)
    disks = os.path.join(output, 'disks')
    os.makedirs(disks, exist_ok=True)
    # Distinct output paths also keep the generated configuration/disk paths
    # independent. A RAM PDisk would invalidate the restart assertion.
    config = KikimrConfigGenerator(
        nodes=1, binary_paths=[binary], output_path=output, static_pdisk_size=8 * 1024**3,
        pdisk_store_path=disks, use_in_memory_pdisks=False, extra_grpc_services=['replication'],
        extra_feature_flags=['enable_async_replication_force_drop'] if force else [])
    return KiKiMR(config, cluster_name=name)


def process_ids(cluster):
    return [node.daemon.process.pid for node in cluster.nodes.values()]


@pytest.mark.parametrize('cascade', [False, True])
def test_force_drop_after_physical_source_outage(cascade):
    """Catches hidden source/credential dependency after actual DONE/restart."""
    os.environ.pop('YDB_USE_IN_MEMORY_PDISKS', None)
    base = yatest.common.output_path('native-cascade' if cascade else 'native-retain')
    os.makedirs(base, exist_ok=True)
    binary = yatest.common.binary_path('ydb/apps/ydbd/ydbd')
    source = make_cluster('source', base, binary, False)
    destination = make_cluster('destination', base, binary, True)
    evidence = {'binary': os.path.realpath(binary), 'cascade': cascade, 'output': base, 'steps': []}
    with open(binary, 'rb') as handle:
        evidence['sha256'] = hashlib.file_digest(handle, 'sha256').hexdigest()
    clients = []
    try:
        source.start()
        destination.start()
        evidence['initial_pids'] = {'source': process_ids(source), 'destination': process_ids(destination)}
        src = driver(source)
        dst = driver(destination)
        clients += [src, dst]
        scheme(src, 'CREATE TABLE `/Root/original` (key Uint32, value Utf8, PRIMARY KEY (key));')
        data(src, 'UPSERT INTO `/Root/original` (key, value) VALUES (7, "replicated"), (9, "before-outage");')
        scheme(dst, 'CREATE SECRET `/Root/source_credential` WITH (value="root@builtin");')
        scheme(dst, f'''CREATE ASYNC REPLICATION `/Root/replication`
            FOR `/Root/original` AS `/Root/replica` WITH (
              CONNECTION_STRING="grpc://localhost:{source.nodes[1].grpc_port}/?database=/Root",
              TOKEN_SECRET_PATH="/Root/source_credential");''')
        expected = [(7, 'replicated'), (9, 'before-outage')]
        wait_for('replicated rows', lambda: rows(dst) == expected)
        before = describe_replication(destination)
        assert before.WhichOneof('state') == 'running', str(before)
        assert len(before.items) == 1
        stream = '/Root/original/' + before.items[0].source_changefeed_name
        evidence['destination_id'] = path_id(destination, '/Root/replica')
        evidence['replication_id'] = path_id(destination, '/Root/replication')
        evidence['source_before'] = source_config(source, stream)
        assert evidence['source_before']['consumers'], evidence
        for node in source.nodes.values():
            node.stop()
        with socket.socket() as probe:
            probe.settimeout(1)
            assert probe.connect_ex(('localhost', source.nodes[1].grpc_port)) != 0
        evidence['steps'].append('source process stopped and endpoint unavailable while replication running')
        scheme(dst, 'DROP SECRET `/Root/source_credential`;')
        # No PAUSE or connection change: direct public active -> DONE.
        scheme(dst, 'ALTER ASYNC REPLICATION `/Root/replication` SET (STATE="DONE", FAILOVER_MODE="FORCE");')
        wait_for('actual controller DONE', lambda: describe_replication(destination).WhichOneof('state') == 'done')
        evidence['steps'].append('actual DONE with source stopped and original valid credential deleted')
        dst.stop()
        destination.restart_nodes()
        evidence['destination_restart_pids'] = process_ids(destination)
        assert evidence['destination_restart_pids'] != evidence['initial_pids']['destination']
        dst = driver(destination)
        clients.append(dst)
        assert describe_replication(destination).WhichOneof('state') == 'done'
        assert rows(dst) == expected
        assert path_id(destination, '/Root/replica') == evidence['destination_id']
        data(dst, 'UPSERT INTO `/Root/replica` (key, value) VALUES (9, "writable-after-restart");')
        scheme(dst, 'DROP ASYNC REPLICATION `/Root/replication` '
               + ('CASCADE ' if cascade else '') + 'FORCE;')
        names = [entry.name for entry in dst.scheme_client.list_directory('/Root').children]
        assert 'replication' not in names
        if cascade:
            assert 'replica' not in names
        else:
            assert path_id(destination, '/Root/replica') == evidence['destination_id']
            assert rows(dst) == [(7, 'replicated'), (9, 'writable-after-restart')]
            data(dst, 'UPSERT INTO `/Root/replica` (key, value) VALUES (9, "writable-after-drop");')
            assert rows(dst) == [(7, 'replicated'), (9, 'writable-after-drop')]
        for node in source.nodes.values():
            node.start()
        wait_for('source restore', lambda: source_config(source, stream) == evidence['source_before'])
        time.sleep(15)  # Cross the source cleanup retry interval before restarting.
        assert source_config(source, stream) == evidence['source_before']
        destination.restart_nodes()
        assert source_config(source, stream) == evidence['source_before']
        evidence['steps'].append('FORCE complete; source restored unchanged; destination restarted again')
    finally:
        with open(os.path.join(base, 'evidence.json'), 'w') as handle:
            json.dump(evidence, handle, indent=2)
        for client in clients:
            client.stop()
        destination.stop()
        source.stop()
        # YA result accumulation expands sparse files. Test data is no longer
        # needed after both processes stop; retain logs/config/evidence only.
        for cluster_name in ('source', 'destination'):
            for filename in os.listdir(os.path.join(base, cluster_name, 'disks')):
                if filename.startswith('pdisk') and filename.endswith('.data'):
                    os.unlink(os.path.join(base, cluster_name, 'disks', filename))
