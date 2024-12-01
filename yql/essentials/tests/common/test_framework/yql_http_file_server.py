import io
import os
import pytest
import threading
import shutil

import six.moves.BaseHTTPServer as BaseHTTPServer
import six.moves.socketserver as socketserver

from yql_ports import get_yql_port, release_yql_port


# handler is created on each request
# store state in server
class YqlHttpRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def get_requested_filename(self):
        return self.path.lstrip('/')

    def do_GET(self):
        f = self.send_head(self.get_requested_filename())
        if f:
            try:
                shutil.copyfileobj(f, self.wfile)
            finally:
                f.close()

    def do_HEAD(self):
        f = self.send_head(self.get_requested_filename())
        if f:
            f.close()

    def get_file_and_size(self, filename):
        try:
            path = self.server.file_paths[filename]
            f = open(path, 'rb')
            fs = os.fstat(f.fileno())
            size = fs[6]
            return (f, size)
        except KeyError:
            try:
                content = self.server.file_contents[filename]
                return (io.BytesIO(content), len(content))
            except KeyError:
                return (None, 0)

        return (None, 0)

    def send_head(self, filename):
        (f, size) = self.get_file_and_size(filename)

        if not f:
            self.send_error(404, "File %s not found" % filename)
            return None

        if self.server.etag is not None:
            if_none_match = self.headers.get('If-None-Match', None)
            if if_none_match == self.server.etag:
                self.send_response(304)
                self.end_headers()
                f.close()
                return None

        self.send_response(200)

        if self.server.etag is not None:
            self.send_header("ETag", self.server.etag)

        self.send_header("Content-type", 'application/octet-stream')
        self.send_header("Content-Length", size)
        self.end_headers()
        return f


class YqlHttpFileServer(socketserver.TCPServer, object):
    def __init__(self):
        self.http_server_port = get_yql_port('YqlHttpFileServer')
        super(YqlHttpFileServer, self).__init__(('', self.http_server_port), YqlHttpRequestHandler,
                                                bind_and_activate=False)
        self.file_contents = {}
        self.file_paths = {}
        # common etag for all resources
        self.etag = None
        self.serve_thread = None

    def start(self):
        self.allow_reuse_address = True
        self.server_bind()
        self.server_activate()
        self.serve_thread = threading.Thread(target=self.serve_forever)
        self.serve_thread.start()

    def stop(self):
        super(YqlHttpFileServer, self).shutdown()
        self.serve_thread.join()
        release_yql_port(self.http_server_port)
        self.http_server_port = None

    def forget_files(self):
        self.register_files({}, {})

    def set_etag(self, newEtag):
        self.etag = newEtag

    def register_new_path(self, key, file_path):
        self.file_paths[key] = file_path
        return self.compose_http_link(key)

    def register_files(self, file_contents, file_paths):
        self.file_contents = file_contents
        self.file_paths = file_paths

        keys = []
        if file_contents:
            keys.extend(file_contents.keys())

        if file_paths:
            keys.extend(file_paths.keys())

        return {k: self.compose_http_link(k) for k in keys}

    def compose_http_link(self, filename):
        return self.compose_http_host() + '/' + filename

    def compose_http_host(self):
        if not self.http_server_port:
            raise Exception('http_server_port is empty. start HTTP server first')

        return 'http://localhost:%d' % self.http_server_port


@pytest.fixture(scope='module')
def yql_http_file_server(request):
    server = YqlHttpFileServer()
    server.start()
    request.addfinalizer(server.stop)
    return server
