import pytest
import struct

from ydb.public.api.protos.draft import fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


def query(alias, id=42):
    return f'''
        $input = SELECT {id}ul AS id;
        $profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
            WITH CONNECTION='{alias}', INPUT_TYPE=Struct<id:Uint64>,
                 OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
        SELECT id, name, score FROM $profiles;
    '''


def start(client, sql):
    return client.create_query('wasm_profile', sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id


def batch_query(alias, ids):
    source = ' UNION ALL '.join(f'SELECT {id}ul AS id' for id in ids)
    return query(alias).replace('SELECT 42ul AS id', source)


def echo_query(method='Echo', output='Struct<value:String,length:Uint64,empty:Bool,delta:Int64>'):
    return f'''
    $input = SELECT "hello" AS message UNION ALL SELECT "" AS message UNION ALL SELECT "hello" AS message;
    $result = PROCESS $input USING EXTERNAL FUNCTION('Echo', '{method}')
        WITH CONNECTION='echo_http', INPUT_TYPE=Struct<message:String>, OUTPUT_TYPE={output};
    SELECT * FROM $result;
    '''


@yq_v1
@pytest.mark.parametrize('batch_config', [{'rows': 1}, {'rows': 2}], indirect=True)
def test_second_module_with_different_schema(client, services, yq_version, batch_config):
    query_id = start(client, echo_query())
    client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    result = client.get_result_data(query_id).result.result_set
    columns = {column.name: i for i, column in enumerate(result.columns)}
    assert sorted((
        row.items[columns['value']].bytes_value,
        row.items[columns['length']].uint64_value,
        row.items[columns['empty']].bool_value,
        row.items[columns['delta']].int64_value,
    ) for row in result.rows) == [(b'', 0, True, 0), (b'hello', 5, False, -5), (b'hello', 5, False, -5)]
    assert len(services.requests) == (3 if batch_config['rows'] == 1 else 2)
    assert all(kind == 'echo' and auth == 'Bearer host-secret' for kind, _, auth in services.requests)
    assert sum(struct.unpack_from('<I', body)[0] for _, body, _ in services.requests) == 3


@yq_v1
@pytest.mark.parametrize('batch_config', [{'rows': 2}], indirect=True)
def test_second_method_with_different_result_schema(client, services, yq_version, batch_config):
    query_id = start(client, echo_query('Length', 'Struct<length:Uint32>'))
    client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    rows = client.get_result_data(query_id).result.result_set.rows
    assert sorted(row.items[0].uint32_value for row in rows) == [0, 5, 5]
    assert len(services.requests) == 2


@yq_v1
def test_unknown_module_method_has_no_network_access(client, services, yq_version):
    query_id = start(client, echo_query('Missing'))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert 'Unknown WASM service method' in str(client.describe_query(query_id).result.query.issue)
    assert not services.requests


@yq_v1
def test_second_module_wrong_schema_has_no_network_access(client, services, yq_version):
    query_id = start(client, echo_query(output='Struct<length:Uint64>'))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert not services.requests


@yq_v1
def test_second_module_oversized_input_has_no_network_access(client, services, yq_version):
    oversized = '"' + 'x' * 1025 + '"'
    sql = echo_query().replace('"hello"', oversized).replace('""', oversized)
    query_id = start(client, sql)
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert not services.requests


@yq_v1
@pytest.mark.parametrize('batch_config', [{'rows': 2, 'echo_small_result_limit': True}], indirect=True)
def test_host_rejects_module_result_exceeding_manifest(client, services, yq_version, batch_config):
    query_id = start(client, echo_query())
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    issues = str(client.describe_query(query_id).result.query.issue)
    assert 'Invalid WASM service result framing' in issues
    assert 'host-secret' not in issues
    assert len(services.requests) == 1


@yq_v1
@pytest.mark.parametrize('batch_config', [{'rows': 64, 'bytes': 592}], indirect=True)
@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_batch_profiles(client, services, protocol, yq_version, batch_config):
    query_id = start(client, batch_query(f'profiles_{protocol}', [42, 42, 43, 44, 45]))
    client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    rows = client.get_result_data(query_id).result.result_set.rows
    assert sorted((row.items[0].uint64_value, row.items[1].text_value, row.items[2].uint32_value) for row in rows) == [
        (42, 'Ada', 97), (42, 'Ada', 97), (43, 'Ada', 97), (44, 'Ada', 97), (45, 'Ada', 97),
    ]
    assert len(services.requests) == 3
    assert sorted(len(request[1]) for request in services.requests) == [1, 2, 2]
    assert sorted(id for _, ids, _ in services.requests for id in ids) == [42, 42, 43, 44, 45]
    assert all(kind == protocol and auth == 'Bearer host-secret' for kind, _, auth in services.requests)


@yq_v1
@pytest.mark.parametrize('batch_config', [{'rows': 2}], indirect=True)
@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_batch_bad_response(client, services, protocol, yq_version, batch_config):
    query_id = start(client, batch_query(f'profiles_{protocol}', [42, 401]))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert len(services.requests) == 1
    issues = str(client.describe_query(query_id).result.query.issue)
    assert 'WASM service failed' in issues
    assert 'host-secret' not in issues


@yq_v1
@pytest.mark.parametrize('batch_config', [{'rows': 2}], indirect=True)
def test_batch_deadline(client, services, yq_version, batch_config):
    query_id = start(client, batch_query('profiles_http', [42, 999]))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert services.started.is_set()
    assert len(services.requests) == 1


@yq_v1
@pytest.mark.parametrize('batch_config', [{'rows': 2}], indirect=True)
def test_batch_cancellation(client, services, yq_version, batch_config):
    query_id = start(client, batch_query('profiles_grpc', [42, 999]))
    assert services.started.wait(30)
    client.abort_query(query_id)
    client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)
    assert services.cancelled.wait(10)
    assert len(services.requests) == 1


@yq_v1
@pytest.mark.parametrize('batch_config', [{'rows': 2}], indirect=True)
def test_batch_empty_input(client, services, yq_version, batch_config):
    sql = query('profiles_http').replace('SELECT 42ul AS id', 'SELECT id FROM (SELECT 42ul AS id) WHERE FALSE')
    query_id = start(client, sql)
    client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    assert not client.get_result_data(query_id).result.result_set.rows
    assert not services.requests


@yq_v1
@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_query_typed_profile(client, services, protocol, yq_version):
    query_id = start(client, query(f'profiles_{protocol}'))
    client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    result = client.get_result_data(query_id).result.result_set
    assert [column.name for column in result.columns] == ['id', 'name', 'score']
    assert len(result.rows) == 1
    row = result.rows[0].items
    assert (row[0].uint64_value, row[1].text_value, row[2].uint32_value) == (42, 'Ada', 97)
    assert services.requests == [(protocol, 42, 'Bearer host-secret')]
    description = client.describe_query(query_id).result.query
    assert 'host-secret' not in description.ast.data
    assert 'host-secret' not in description.plan.json


@yq_v1
def test_multiple_input_rows(client, services, yq_version):
    sql = query('profiles_http').replace('SELECT 42ul AS id', 'SELECT 42ul AS id UNION ALL SELECT 43ul AS id')
    query_id = start(client, sql)
    client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    rows = client.get_result_data(query_id).result.result_set.rows
    assert sorted((row.items[0].uint64_value, row.items[1].text_value, row.items[2].uint32_value) for row in rows) == [
        (42, 'Ada', 97), (43, 'Ada', 97),
    ]
    assert sorted(services.requests) == [('http', 42, 'Bearer host-secret'), ('http', 43, 'Bearer host-secret')]


@yq_v1
def test_empty_input_has_no_network_access(client, services, yq_version):
    sql = query('profiles_http').replace('SELECT 42ul AS id', 'SELECT id FROM (SELECT 42ul AS id) WHERE FALSE')
    query_id = start(client, sql)
    client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    assert not client.get_result_data(query_id).result.result_set.rows
    assert not services.requests


@yq_v1
def test_unknown_alias_has_no_network_access(client, services, yq_version):
    query_id = start(client, query('missing'))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert 'Unknown WASM service connection alias' in str(client.describe_query(query_id).result.query.issue)
    assert not services.requests


@yq_v1
def test_malformed_response_fails_query(client, yq_version):
    query_id = start(client, query('profiles_http', 400))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    issues = str(client.describe_query(query_id).result.query.issue)
    assert 'WASM service failed' in issues
    assert 'host-secret' not in issues


@yq_v1
def test_query_deadline(client, services, yq_version):
    query_id = start(client, query('profiles_http', 999))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert services.started.is_set()


@yq_v1
def test_query_cancellation(client, services, yq_version):
    query_id = start(client, query('profiles_grpc', 999))
    assert services.started.wait(30)
    client.abort_query(query_id)
    client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)
    assert services.cancelled.wait(10)


@yq_v1
def test_invalid_row_type_has_no_network_access(client, services, yq_version):
    sql = query('profiles_http').replace('score:Uint32', 'score:String')
    query_id = start(client, sql)
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert not services.requests
