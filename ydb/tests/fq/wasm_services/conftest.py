import concurrent.futures
import http.server
import json
import ssl
import threading

import grpc
import pytest
import yatest.common

from ydb.public.api.protos.draft import fq_pb2 as fq
from ydb.udfs.wasm.profile.proto.schema import profile_pb2
from ydb.tests.tools.fq_runner.custom_hooks import *  # noqa: F401,F403
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.library.harness.tls_tools import generate_selfsigned_cert
from ydb.tests.tools.fq_runner.kikimr_utils import (
    DefaultConfigExtension, ComputeExtension, ExtensionPoint, YQv2Extension, start_kikimr,
)


class MockServices:
    def __init__(self, tls=False):
        self.requests = []
        self.lock = threading.Lock()
        self.started = threading.Event()
        self.release = threading.Event()
        self.cancelled = threading.Event()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                if self.path == '/echo':
                    with owner.lock:
                        owner.requests.append(('echo', body, self.headers.get('Authorization')))
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                request = json.loads(body)
                batch = 'ids' in request
                ids = request['ids'] if batch else [request['id']]
                with owner.lock:
                    owner.requests.append(('http', tuple(ids) if batch else ids[0], self.headers.get('Authorization')))
                if 999 in ids:
                    owner.started.set()
                    owner.release.wait(30)
                profiles = [{'id': id, 'name': 'Ada', 'score': 97, 'version': 1} for id in ids]
                if batch and 401 in ids:
                    profiles = profiles[:-1]
                payload = b'not json' if 400 in ids else json.dumps(profiles if batch else profiles[0]).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                try:
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *args):
                pass

        self.http = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.tls = tls
        self.ca_certificate = ''
        cert, key = (b'', b'')
        if tls:
            cert, key = generate_selfsigned_cert('localhost')
            self.ca_certificate = cert.decode()
            cert_path = yatest.common.output_path('mock-service.crt')
            key_path = yatest.common.output_path('mock-service.key')
            with open(cert_path, 'wb') as output:
                output.write(cert)
            with open(key_path, 'wb') as output:
                output.write(key)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert_path, key_path)
            self.http.socket = context.wrap_socket(self.http.socket, server_side=True)
        self.thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.thread.start()
        self.grpc = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=2))
        self.grpc.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            'NFq.NWasmServices.NTest.MockService', {'Lookup': grpc.unary_unary_rpc_method_handler(
                self.lookup, request_deserializer=profile_pb2.ProfileRequest.FromString,
                response_serializer=profile_pb2.ProfileReply.SerializeToString,
            ), 'LookupBatch': grpc.unary_unary_rpc_method_handler(
                self.lookup_batch, request_deserializer=profile_pb2.ProfileBatchRequest.FromString,
                response_serializer=profile_pb2.ProfileBatchReply.SerializeToString,
            )}),))
        self.grpc_port = self.grpc.add_secure_port('127.0.0.1:0', grpc.ssl_server_credentials(((key, cert),))) if tls else \
            self.grpc.add_insecure_port('127.0.0.1:0')
        self.grpc.start()

    def lookup(self, request, context):
        with self.lock:
            self.requests.append(('grpc', request.id, dict(context.invocation_metadata()).get('authorization')))
        if request.id == 999:
            context.add_callback(self.cancelled.set)
            self.started.set()
            self.cancelled.wait(30)
        profile = profile_pb2.Profile(id=request.id, name=b'Ada', score=97, version=1)
        return profile_pb2.ProfileReply(payload=profile.SerializeToString())

    def lookup_batch(self, request, context):
        with self.lock:
            self.requests.append(('grpc', tuple(request.ids), dict(context.invocation_metadata()).get('authorization')))
        if 999 in request.ids:
            context.add_callback(self.cancelled.set)
            self.started.set()
            self.cancelled.wait(30)
        ids = request.ids[:-1] if 401 in request.ids else request.ids
        profiles = profile_pb2.ProfileBatchPayload(profiles=[
            profile_pb2.Profile(id=id, name=b'Ada', score=97, version=1) for id in ids
        ])
        return profile_pb2.ProfileBatchReply(payload=profiles.SerializeToString())

    def stop(self):
        self.release.set()
        self.cancelled.set()
        self.grpc.stop(0).wait(10)
        self.http.shutdown()
        self.http.server_close()
        self.thread.join(10)


class WasmExtension(ExtensionPoint):
    def __init__(self, timeout_ms, batch_config):
        self.timeout_ms = timeout_ms
        self.batch_config = batch_config

    def is_applicable(self, request):
        return True

    def apply_to_kikimr_conf(self, request, configuration):
        pass

    def apply_to_kikimr(self, request, kikimr):
        config = {
            'enabled': True,
            'modules': [
                {'module_path': yatest.common.source_path('ydb/udfs/wasm/profile/ut/data/profile.wasm'),
                 'manifest_path': yatest.common.source_path('ydb/udfs/wasm/profile/manifest.json')},
                {'module_path': yatest.common.source_path('ydb/udfs/wasm/echo/ut/data/echo.wasm'),
                 'manifest_path': yatest.common.source_path('ydb/udfs/wasm/echo/manifest.json')},
            ],
            'call_timeout_ms': self.timeout_ms,
            'max_batch_rows': self.batch_config.get('rows', 1),
            'max_batch_bytes': self.batch_config.get('bytes', 32768),
        }
        if self.batch_config.get('echo_small_result_limit'):
            with open(config['modules'][1]['manifest_path']) as source:
                manifest = json.load(source)
            manifest['service_methods'][0]['output'][0]['max_bytes'] = 1
            path = yatest.common.output_path(request.node.name + '_echo_manifest.json')
            with open(path, 'w') as output:
                json.dump(manifest, output)
            config['modules'][1]['manifest_path'] = path
        for tenant in kikimr.tenants.values():
            tenant.fq_config['wasm_services'] = config
        kikimr.control_plane.fq_config['control_plane_storage']['available_connection'].append('EXTERNAL_SERVICE')


@pytest.fixture
def services(request):
    mock = MockServices(tls=getattr(request, 'param', False))
    try:
        yield mock
    finally:
        mock.stop()


@pytest.fixture
def batch_config(request):
    return getattr(request, 'param', {})


@pytest.fixture
def kikimr(request, services, yq_version, batch_config):
    timeout = 200 if 'deadline' in request.node.originalname else 30000
    extensions = [DefaultConfigExtension(''), YQv2Extension(yq_version), ComputeExtension(),
                  WasmExtension(timeout, batch_config)]
    with start_kikimr(request, extensions) as cluster:
        cluster.control_plane.wait_bootstrap(1)
        yield cluster


@pytest.fixture
def client(kikimr, services, batch_config):
    client = FederatedQueryClient('my_folder', streaming_over_kikimr=kikimr)
    batch = batch_config.get('rows', 1) > 1
    host = 'localhost' if services.tls else '127.0.0.1'
    scheme = 'https' if services.tls else 'http'
    client.create_external_service_connection(
        'profiles_http', f'{scheme}://{host}:{services.http.server_port}/profile' + ('/batch' if batch else ''),
        insecure=not services.tls, ca_certificate=services.ca_certificate, token='host-secret',
    )
    client.create_external_service_connection(
        'profiles_grpc', f'{host}:{services.grpc_port}', protocol=fq.ExternalService.GRPC,
        method='/NFq.NWasmServices.NTest.MockService/Lookup' + ('Batch' if batch else ''),
        insecure=not services.tls, ca_certificate=services.ca_certificate, token='host-secret',
    )
    client.create_external_service_connection(
        'echo_http', f'{scheme}://{host}:{services.http.server_port}/echo', insecure=not services.tls,
        ca_certificate=services.ca_certificate, token='host-secret',
        headers={'Content-Type': 'application/octet-stream'},
    )
    return client
