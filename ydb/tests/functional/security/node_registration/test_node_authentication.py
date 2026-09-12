import grpc
import pytest

from google.protobuf import text_format
from helpers import YdbGrpcLog, canonical_log, registration_result

from ydb.core.protos import grpc_pb2_grpc, msgbus_pb2
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
from ydb.public.api.protos import ydb_discovery_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


# A client certificate cannot be transmitted over plaintext gRPC.
AUTH_CASES = [
    pytest.param('allowed', None, id='certificate-only'),
    pytest.param(None, 'root@builtin', id='token-only'),
    pytest.param('allowed', 'root@builtin', id='certificate-and-token'),
    pytest.param(None, None, id='no-credentials'),
    pytest.param(None, 'denied@builtin', id='denied-token'),
    pytest.param('denied', None, id='denied-certificate'),
    pytest.param('allowed', 'denied@builtin', id='denied-token-over-certificate'),
    pytest.param('allowed', 'invalid-token', id='invalid-token-over-certificate'),
    pytest.param('denied', 'root@builtin', id='token-over-denied-certificate'),
    pytest.param('unrecognized', 'root@builtin', id='token-over-unrecognized-certificate'),
]


def open_channel(cluster, certificates, protocol, certificate):
    node = cluster.nodes[1]
    if protocol == 'grpc':
        return grpc.insecure_channel(f'localhost:{node.port}')
    cert, key = certificates[certificate] if certificate else (None, None)
    credentials = grpc.ssl_channel_credentials(
        root_certificates=certificates['ca'], private_key=key, certificate_chain=cert,
    )
    return grpc.secure_channel(f'localhost:{node.grpc_ssl_port}', credentials)


def credentials_for_request(request, token, token_location):
    if token is None:
        return ()
    if token_location == 'body':
        request.SecurityToken = token
        return ()
    return (('x-ydb-auth-ticket', token),)


def invoke(call, request, metadata):
    try:
        return call(request, metadata=metadata, timeout=30), {'grpc_status': 'OK'}
    except grpc.RpcError as error:
        return None, {'grpc_status': error.code().name, 'details': error.details()}


@pytest.mark.parametrize('protocol', ['grpc', 'grpcs'])
@pytest.mark.parametrize('certificate,token', AUTH_CASES)
@pytest.mark.parametrize('api,token_location', [
    pytest.param('discovery', 'metadata', id='discovery'),
    pytest.param('legacy', 'body', id='legacy-token-in-body'),
    pytest.param('legacy', 'metadata', id='legacy-token-in-metadata'),
])
def test_node_registration(
    cluster,
    certificates,
    registration_port,
    protocol,
    certificate,
    token,
    api,
    token_location,
    tmp_path
):
    # Re-register the same endpoint to exercise the normal lease renewal as well.
    host, port, address = 'localhost', registration_port, '127.0.0.1'
    with open_channel(cluster, certificates, protocol, certificate) as channel:
        if api == 'discovery':
            request = ydb_discovery_pb2.NodeRegistrationRequest(
                host=host, port=port, resolve_host=host, address=address, domain_path='Root',
            )
            call = ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub(channel).NodeRegistration
        else:
            request = msgbus_pb2.TNodeRegistrationRequest(
                Host=host, Port=port, ResolveHost=host, Address=address, DomainPath='Root',
            )
            call = grpc_pb2_grpc.TGRpcServerStub(channel).RegisterNode
        metadata = credentials_for_request(request, token, token_location)
        if api == 'discovery':
            metadata += (('x-ydb-database', '/Root'),)
        server_log = YdbGrpcLog(cluster)
        response, log = invoke(call, request, metadata)
        log['ydb_log'] = server_log.response('NodeRegistration' if api == 'discovery' else 'RegisterNode')

    if response is not None:
        log.update(registration_result(response, api, cluster.nodes[1].node_id, port))
    return canonical_log(tmp_path, log)


@pytest.mark.parametrize('protocol', ['grpc', 'grpcs'])
@pytest.mark.parametrize('certificate,token', AUTH_CASES)
@pytest.mark.parametrize('token_location', ['body', 'metadata'])
def test_get_node_config(cluster, certificates, node_config, protocol, certificate, token,
                         token_location, tmp_path):
    request = msgbus_pb2.TConsoleRequest(DomainName='Root')
    request.GetNodeConfigRequest.Node.Host = 'localhost'
    request.GetNodeConfigRequest.Node.NodeType = 'node-auth-test'
    metadata = credentials_for_request(request, token, token_location)
    with open_channel(cluster, certificates, protocol, certificate) as channel:
        call = grpc_pb2_grpc.TGRpcServerStub(channel).ConsoleRequest
        server_log = YdbGrpcLog(cluster)
        response, log = invoke(call, request, metadata)
        log['ydb_log'] = server_log.response('ConsoleRequest')
    if response is not None:
        log['status'] = StatusIds.StatusCode.Name(response.Status.Code)
        log['response_present'] = response.HasField('GetNodeConfigResponse')
        if log['response_present']:
            result = response.GetNodeConfigResponse
            log['config_status'] = StatusIds.StatusCode.Name(result.Status.Code)
            log['log_config'] = text_format.MessageToString(result.Config.LogConfig)
    return canonical_log(tmp_path, log)
