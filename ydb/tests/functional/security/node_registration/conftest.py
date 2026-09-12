from datetime import datetime, timedelta

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from helpers import YdbGrpcLog

from library.python.port_manager import PortManager
from ydb.core.protos import grpc_pb2_grpc, msgbus_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.tls_tools import generate_selfsigned_cert
from ydb.tests.library.harness.util import LogLevels


@pytest.fixture(scope='module')
def certificates():
    ca_pem, ca_key_pem = generate_selfsigned_cert('localhost')
    ca = x509.load_pem_x509_certificate(ca_pem)
    ca_key = serialization.load_pem_private_key(ca_key_pem, password=None)
    result = {'ca': ca_pem, 'server': (ca_pem, ca_key_pem)}
    for name in ('allowed', 'denied', 'unrecognized'):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        now = datetime.utcnow()
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
            .issuer_name(ca.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .sign(ca_key, hashes.SHA256())
        )
        result[name] = (
            cert.public_bytes(serialization.Encoding.PEM),
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            ),
        )
    return result


@pytest.fixture(scope='module')
def cluster(certificates, tmp_path_factory, request):
    tls_dir = tmp_path_factory.mktemp('node_auth_tls')
    (tls_dir / 'ca.pem').write_bytes(certificates['ca'])
    (tls_dir / 'cert.pem').write_bytes(certificates['server'][0])
    (tls_dir / 'key.pem').write_bytes(certificates['server'][1])
    config = KikimrConfigGenerator(
        nodes=1,
        grpc_ssl_enable=True,
        grpc_tls_data_path=str(tls_dir),
        generate_grpc_tls_data=False,
        enforce_user_token_requirement=True,
        default_clusteradmin='root@builtin',
        additional_log_configs={'GRPC_SERVER': LogLevels.DEBUG},
    )
    security = config.yaml_config['domains_config']['security_config']
    # A nonempty allowlist is essential: otherwise anonymous registration is allowed.
    security['register_dynamic_node_allowed_sids'] = getattr(
        request, 'param', ['root@builtin', 'dynamic-nodes@cert'],
    )
    config.yaml_config.setdefault('auth_config', {})['staff_api_user_token'] = 'root@builtin'
    config.yaml_config['client_certificate_authorization'] = {
        'request_client_certificate': True,
        'client_certificate_definitions': [
            {
                'subject_terms': [{'short_name': 'CN', 'values': ['allowed']}],
                'member_groups': ['dynamic-nodes@cert'],
            },
            {
                'subject_terms': [{'short_name': 'CN', 'values': ['denied']}],
                'member_groups': ['other-clients@cert'],
            },
        ],
    }
    cluster = KiKiMR(config)
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture(scope='module')
def registration_port():
    with PortManager() as ports:
        yield ports.get_port()


@pytest.fixture(scope='module')
def node_config(cluster):
    # Install a recognizable config for the test node type, without changing the server.
    request = msgbus_pb2.TConsoleRequest(SecurityToken='root@builtin')
    item = request.ConfigureRequest.Actions.add().AddConfigItem.ConfigItem
    item.UsageScope.TenantAndNodeTypeFilter.NodeType = 'node-auth-test'
    item.Config.LogConfig.DefaultLevel = 5
    with grpc.insecure_channel(f'localhost:{cluster.nodes[1].port}') as channel:
        server_log = YdbGrpcLog(cluster)
        response = grpc_pb2_grpc.TGRpcServerStub(channel).ConsoleRequest(request, timeout=30)
        # Flush the setup response before tests capture the same RPC method.
        server_log.response('ConsoleRequest')
    if response.Status.Code != StatusIds.SUCCESS:
        pytest.fail(f'Could not install the test config: {response}')
    return item.Config
