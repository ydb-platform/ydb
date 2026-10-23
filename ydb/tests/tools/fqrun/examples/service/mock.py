"""Local Profile HTTP/gRPC and Echo services for fqrun examples and tests."""
import concurrent.futures
import http.server
import json
import ssl
import threading
from pathlib import Path

import grpc
from ydb.udfs.wasm.profile.proto.schema import profile_pb2


class MockServices:
    def __init__(self, http_port=0, grpc_port=0, certificate=None, private_key=None):
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

        self.http = http.server.ThreadingHTTPServer(('127.0.0.1', http_port), Handler)
        self.tls = certificate is not None
        self.ca_certificate = Path(certificate).read_text() if certificate else ''
        cert = self.ca_certificate.encode()
        key = Path(private_key).read_bytes() if private_key else b''
        if self.tls:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(certificate, private_key)
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
        self.grpc_port = self.grpc.add_secure_port(f'127.0.0.1:{grpc_port}', grpc.ssl_server_credentials(((key, cert),))) if self.tls else \
            self.grpc.add_insecure_port(f'127.0.0.1:{grpc_port}')
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
