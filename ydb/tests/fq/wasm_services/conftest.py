import concurrent.futures
import http.server
import json
import threading

import grpc
import pytest
import yatest.common

from ydb.core.fq.libs.wasm_services.ut.protos import profile_pb2
from ydb.tests.tools.fq_runner.custom_hooks import *  # noqa: F401,F403
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.fq_runner.kikimr_utils import (
    DefaultConfigExtension, ComputeExtension, ExtensionPoint, YQv2Extension, start_kikimr,
)


class MockServices:
    def __init__(self):
        self.requests = []
        self.lock = threading.Lock()
        self.started = threading.Event()
        self.release = threading.Event()
        self.cancelled = threading.Event()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
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
        self.grpc_port = self.grpc.add_insecure_port('127.0.0.1:0')
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
    def __init__(self, services, timeout_ms, batch_config):
        self.services = services
        self.timeout_ms = timeout_ms
        self.batch_config = batch_config

    def is_applicable(self, request):
        return True

    def apply_to_kikimr_conf(self, request, configuration):
        pass

    def apply_to_kikimr(self, request, kikimr):
        config = {
            'enabled': True,
            'module_path': yatest.common.source_path('ydb/core/fq/libs/wasm_services/ut/data/transport_coroutine.wasm'),
            'call_timeout_ms': self.timeout_ms,
            'max_batch_rows': self.batch_config.get('rows', 1),
            'max_batch_bytes': self.batch_config.get('bytes', 32768),
            'bindings': [
                {'alias': 'profiles_http', 'endpoint': f'http://127.0.0.1:{self.services.http.server_port}/profile',
                 'headers': {'Authorization': 'Bearer host-secret'}},
                {'alias': 'profiles_grpc', 'protocol': 'GRPC', 'endpoint': f'127.0.0.1:{self.services.grpc_port}',
                 'method': '/NFq.NWasmServices.NTest.MockService/Lookup', 'grpc_insecure': True,
                 'headers': {'authorization': 'Bearer host-secret'}},
            ],
        }
        if self.batch_config.get('rows', 1) > 1:
            config['bindings'][0]['endpoint'] += '/batch'
            config['bindings'][1]['method'] += 'Batch'
        for tenant in kikimr.tenants.values():
            tenant.fq_config['wasm_services'] = config


@pytest.fixture
def services():
    mock = MockServices()
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
                  WasmExtension(services, timeout, batch_config)]
    with start_kikimr(request, extensions) as cluster:
        cluster.control_plane.wait_bootstrap(1)
        yield cluster


@pytest.fixture
def client(kikimr):
    return FederatedQueryClient('my_folder', streaming_over_kikimr=kikimr)
