import grpc
import pytest

from ydb.core.protos import grpc_pb2_grpc, msgbus_pb2, node_broker_pb2
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
from ydb.public.api.protos import ydb_discovery_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


# Each case specifies the expected result on TLS and on plaintext separately.
# A client certificate cannot be transmitted over plaintext gRPC.
AUTH_CASES = [
    pytest.param('allowed', None, 'success', 'unauthenticated', id='certificate-only'),
    pytest.param(None, 'root@builtin', 'success', 'success', id='token-only'),
    pytest.param('allowed', 'root@builtin', 'success', 'success', id='certificate-and-token'),
    pytest.param(None, None, 'unauthenticated', 'unauthenticated', id='no-credentials'),
    pytest.param(None, 'denied@builtin', 'unauthorized', 'unauthorized', id='denied-token'),
    pytest.param('denied', None, 'unauthorized', 'unauthenticated', id='denied-certificate'),
    pytest.param('allowed', 'denied@builtin', 'unauthorized', 'unauthorized', id='denied-token-over-certificate'),
    pytest.param('allowed', 'invalid-token', 'unauthenticated', 'unauthenticated', id='invalid-token-over-certificate'),
    pytest.param('denied', 'root@builtin', 'success', 'success', id='token-over-denied-certificate'),
    pytest.param('unrecognized', 'root@builtin', 'success', 'success', id='token-over-unrecognized-certificate'),
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


def invoke(call, request, metadata, expected):
    if expected == 'unauthenticated':
        with pytest.raises(grpc.RpcError) as error:
            call(request, metadata=metadata, timeout=30)
        assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED, error.value
        return None
    return call(request, metadata=metadata, timeout=30)


@pytest.mark.parametrize('protocol', ['grpc', 'grpcs'])
@pytest.mark.parametrize('certificate,token,tls_result,plaintext_result', AUTH_CASES)
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
    tls_result,
    plaintext_result,
    api,
    token_location
):
    expected = tls_result if protocol == 'grpcs' else plaintext_result
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
        response = invoke(call, request, metadata, expected)

    if response is None:
        return
    if api == 'discovery':
        assert response.operation.ready, response
        status = StatusIds.SUCCESS if expected == 'success' else StatusIds.UNAUTHORIZED
        assert response.operation.status == status, response
        result = ydb_discovery_pb2.NodeRegistrationResult()
        assert response.operation.result.Unpack(result), response
        if expected == 'success':
            assert result.node_id > cluster.nodes[1].node_id, result
            assert result.expire > 0, result
            assert any(n.node_id == result.node_id and n.port == port for n in result.nodes), result
        else:
            assert result.node_id == 0, result
            assert not result.nodes, result
            assert any(issue.message == 'Cannot authorize node. Access denied' for issue in response.operation.issues), response
    else:
        status = node_broker_pb2.TStatus.OK if expected == 'success' else node_broker_pb2.TStatus.UNAUTHORIZED
        assert response.Status.Code == status, response
        if expected == 'success':
            assert response.NodeId > cluster.nodes[1].node_id, response
            assert response.Expire > 0, response
            assert any(n.NodeId == response.NodeId and n.Port == port for n in response.Nodes), response
        else:
            assert not response.HasField('NodeId'), response
            assert not response.Nodes, response


@pytest.mark.parametrize('protocol', ['grpc', 'grpcs'])
@pytest.mark.parametrize('certificate,token,tls_result,plaintext_result', AUTH_CASES)
@pytest.mark.parametrize('token_location', ['body', 'metadata'])
def test_get_node_config(cluster, certificates, node_config, protocol, certificate, token,
                         tls_result, plaintext_result, token_location):
    expected = tls_result if protocol == 'grpcs' else plaintext_result
    request = msgbus_pb2.TConsoleRequest(DomainName='Root')
    request.GetNodeConfigRequest.Node.Host = 'localhost'
    request.GetNodeConfigRequest.Node.NodeType = 'node-auth-test'
    metadata = credentials_for_request(request, token, token_location)
    with open_channel(cluster, certificates, protocol, certificate) as channel:
        call = grpc_pb2_grpc.TGRpcServerStub(channel).ConsoleRequest
        response = invoke(call, request, metadata, expected)
    if response is None:
        return
    status = StatusIds.SUCCESS if expected == 'success' else StatusIds.UNAUTHORIZED
    assert response.Status.Code == status, response
    if expected == 'success':
        assert response.HasField('GetNodeConfigResponse'), response
        assert response.GetNodeConfigResponse.Status.Code == StatusIds.SUCCESS, response
        assert response.GetNodeConfigResponse.Config.LogConfig == node_config.LogConfig, response
    else:
        assert not response.HasField('GetNodeConfigResponse'), response
