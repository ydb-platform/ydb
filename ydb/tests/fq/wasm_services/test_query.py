import pytest

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
    assert 'Unknown WASM Profile connection alias' in str(client.describe_query(query_id).result.query.issue)
    assert not services.requests


@yq_v1
def test_malformed_response_fails_query(client, yq_version):
    query_id = start(client, query('profiles_http', 400))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    issues = str(client.describe_query(query_id).result.query.issue)
    assert 'WASM Profile service failed' in issues
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
