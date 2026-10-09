import pytest
import struct

from ydb.public.api.protos.draft import fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient


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
    assert 'Unknown or inaccessible external service connection' in str(client.describe_query(query_id).result.query.issue)
    assert not services.requests


@yq_v1
@pytest.mark.parametrize('services', [True], indirect=True)
@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_connection_tls(client, services, protocol, yq_version):
    query_id = start(client, query(f'profiles_{protocol}'))
    client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    assert services.requests == [(protocol, 42, 'Bearer host-secret')]


@yq_v1
@pytest.mark.parametrize('services', [True], indirect=True)
@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_connection_untrusted_tls(client, services, protocol, yq_version):
    endpoint = f'https://localhost:{services.http.server_port}/profile' if protocol == 'http' else f'localhost:{services.grpc_port}'
    client.create_external_service_connection(
        'untrusted', endpoint, protocol=fq.ExternalService.HTTP if protocol == 'http' else fq.ExternalService.GRPC,
        method='' if protocol == 'http' else '/NFq.NWasmServices.NTest.MockService/Lookup', token='host-secret',
    )
    query_id = start(client, query('untrusted'))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert 'WASM service failed' in str(client.describe_query(query_id).result.query.issue)
    assert not services.requests
    assert 'host-secret' not in str(client.describe_query(query_id).result.query)


@yq_v1
@pytest.mark.parametrize('services', [True], indirect=True)
@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_connection_tls_hostname_verification(client, services, protocol, yq_version):
    endpoint = f'https://127.0.0.1:{services.http.server_port}/profile' if protocol == 'http' else f'127.0.0.1:{services.grpc_port}'
    client.create_external_service_connection(
        'wrong_hostname', endpoint, protocol=fq.ExternalService.HTTP if protocol == 'http' else fq.ExternalService.GRPC,
        method='' if protocol == 'http' else '/NFq.NWasmServices.NTest.MockService/Lookup',
        ca_certificate=services.ca_certificate, token='host-secret',
    )
    query_id = start(client, query('wrong_hostname'))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert 'WASM service failed' in str(client.describe_query(query_id).result.query.issue)
    assert not services.requests


@yq_v1
def test_connection_scope_isolation(client, kikimr, services, yq_version):
    other = FederatedQueryClient('other_folder', streaming_over_kikimr=kikimr)
    query_id = start(other, query('profiles_http'))
    other.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert 'Unknown or inaccessible external service connection' in str(other.describe_query(query_id).result.query.issue)
    assert not services.requests


@yq_v1
def test_connection_private_visibility(client, kikimr, services, yq_version):
    class Credentials:
        def auth_metadata(self):
            return [('x-ydb-auth-ticket', 'other@builtin')]

    other = FederatedQueryClient('my_folder', streaming_over_kikimr=kikimr, credentials=Credentials())
    query_id = start(other, query('profiles_http'))
    other.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert 'Unknown or inaccessible external service connection' in str(other.describe_query(query_id).result.query.issue)
    assert not services.requests


@yq_v1
def test_connection_wrong_type_has_no_network_access(client, services, yq_version):
    client.create_storage_connection('not_a_service', 'bucket')
    query_id = start(client, query('not_a_service'))
    client.wait_query_status(query_id, fq.QueryMeta.FAILED)
    assert not services.requests


@yq_v1
def test_connection_api_hides_credentials(client, services, yq_version):
    created = client.create_external_service_connection(
        'secret_service', f'http://127.0.0.1:{services.http.server_port}/profile', insecure=True,
        token='hidden-token', headers={'X-Api-Key': 'hidden-header'},
    ).result.connection_id
    response = client.service.DescribeConnection(fq.DescribeConnectionRequest(connection_id=created), metadata=client._create_meta())
    result = fq.DescribeConnectionResult()
    response.operation.result.Unpack(result)
    assert not response.operation.issues
    assert result.connection.content.setting.external_service.auth.HasField('token')
    assert not result.connection.content.setting.external_service.auth.token.token
    assert not result.connection.content.setting.external_service.headers
    listed = client.list_connections(fq.Acl.PRIVATE).result
    assert 'hidden-token' not in str(listed)
    assert 'hidden-header' not in str(listed)


@yq_v1
@pytest.mark.parametrize('current_iam', [False, True])
def test_connection_auth_modes(client, services, yq_version, current_iam):
    request = fq.CreateConnectionRequest()
    request.content.name = 'auth_service'
    request.content.acl.visibility = fq.Acl.PRIVATE
    service = request.content.setting.external_service
    service.protocol = fq.ExternalService.HTTP
    service.endpoint = f'http://127.0.0.1:{services.http.server_port}/profile'
    service.insecure = True
    if current_iam:
        service.auth.current_iam.SetInParent()
    else:
        service.auth.none.SetInParent()
    client.create_connection(request)
    query_id = start(client, query('auth_service'))
    client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    assert services.requests == [('http', 42, 'Bearer root@builtin' if current_iam else None)]


@yq_v1
def test_scope_visible_connection_can_be_used_by_another_user(client, kikimr, services, yq_version):
    class Credentials:
        def auth_metadata(self):
            return [('x-ydb-auth-ticket', 'other@builtin')]

    client.create_external_service_connection(
        'shared_service', f'http://127.0.0.1:{services.http.server_port}/profile', insecure=True,
        token='host-secret', visibility=fq.Acl.SCOPE,
    )
    other = FederatedQueryClient('my_folder', streaming_over_kikimr=kikimr, credentials=Credentials())
    query_id = start(other, query('shared_service'))
    other.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
    assert services.requests == [('http', 42, 'Bearer host-secret')]


@yq_v1
def test_connection_modification_is_used_by_next_query(client, services, yq_version):
    created = client.create_external_service_connection(
        'mutable_service', f'http://127.0.0.1:{services.http.server_port}/profile', insecure=True, token='first-token',
    ).result.connection_id
    first = start(client, query('mutable_service'))
    client.wait_query_status(first, fq.QueryMeta.COMPLETED)
    request = fq.ModifyConnectionRequest(connection_id=created)
    request.content.name = 'mutable_service'
    request.content.acl.visibility = fq.Acl.PRIVATE
    service = request.content.setting.external_service
    service.protocol = fq.ExternalService.HTTP
    service.endpoint = f'http://127.0.0.1:{services.http.server_port}/profile'
    service.insecure = True
    service.auth.token.token = 'second-token'
    client.modify_connection(request)
    second = start(client, query('mutable_service'))
    client.wait_query_status(second, fq.QueryMeta.COMPLETED)
    assert services.requests == [('http', 42, 'Bearer first-token'), ('http', 42, 'Bearer second-token')]


@yq_v1
@pytest.mark.parametrize('endpoint,insecure', [
    ('http://localhost:8080/profile', False), ('https://user:secret@localhost/profile', False),
    ('file:///etc/passwd', False), ('https:///missing-host', False),
])
def test_invalid_connection_has_no_network_access(client, services, yq_version, endpoint, insecure):
    response = client.create_external_service_connection('invalid', endpoint, insecure=insecure, check_issues=False)
    assert response.issues
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
