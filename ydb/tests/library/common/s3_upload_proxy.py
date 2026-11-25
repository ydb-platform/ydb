from contextlib import closing
import os
import select
import socket
from threading import Event

from six.moves import socketserver
from six.moves.urllib.parse import urlsplit


class S3UploadProxy(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, port, paused=False, discard_paused=True):
        endpoint = urlsplit(os.environ["S3_ENDPOINT"])
        self.destination = (endpoint.hostname, endpoint.port)
        self.paused = paused
        self.discard_paused = discard_paused
        self.connected = Event()
        self.release = Event()
        super(S3UploadProxy, self).__init__(("127.0.0.1", port), self.Handler)

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            if self.server.paused:
                self.server.connected.set()
                if not self.server.release.wait(300):
                    return
                if self.server.discard_paused:
                    # Downgrade tests discard requests from the old uploader.
                    return
            with closing(socket.create_connection(self.server.destination, timeout=60)) as backend:
                peers = {self.request: backend, backend: self.request}
                while True:
                    readable, _, _ = select.select(list(peers), [], [], 60)
                    if not readable:
                        return
                    for source in readable:
                        data = source.recv(65536)
                        if not data:
                            return
                        peers[source].sendall(data)
