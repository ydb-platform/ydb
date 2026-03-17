import socketserver as SocketServer
import threading
import unittest

from prometheus_client import CollectorRegistry, Gauge
from prometheus_client.bridge.graphite import GraphiteBridge


def fake_timer():
    return 1434898897.5


class TestGraphiteBridge(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()

        self.data = ''

        class TCPHandler(SocketServer.BaseRequestHandler):
            def handle(s):
                self.data = s.request.recv(1024)

        server = SocketServer.TCPServer(('', 0), TCPHandler)

        class ServingThread(threading.Thread):
            def run(self):
                server.handle_request()
                server.socket.close()

        self.t = ServingThread()
        self.t.start()

        # Explicitly use localhost as the target host, since connecting to 0.0.0.0 fails on Windows
        self.address = ('localhost', server.server_address[1])
        self.gb = GraphiteBridge(self.address, self.registry, _timer=fake_timer)

    def _use_tags(self):
        self.gb = GraphiteBridge(self.address, self.registry, tags=True, _timer=fake_timer)

    def test_nolabels(self):
        gauge = Gauge('g', 'help', registry=self.registry)
        gauge.inc()

        self.gb.push()
        self.t.join()

        self.assertEqual(b'g 1.0 1434898897\n', self.data)

    def test_labels(self):
        labels = Gauge('labels', 'help', ['a', 'b'], registry=self.registry)
        labels.labels('c', 'd').inc()

        self.gb.push()
        self.t.join()

        self.assertEqual(b'labels.a.c.b.d 1.0 1434898897\n', self.data)

    def test_labels_tags(self):
        self._use_tags()
        labels = Gauge('labels', 'help', ['a', 'b'], registry=self.registry)
        labels.labels('c', 'd').inc()

        self.gb.push()
        self.t.join()

        self.assertEqual(b'labels;a=c;b=d 1.0 1434898897\n', self.data)

    def test_prefix(self):
        labels = Gauge('labels', 'help', ['a', 'b'], registry=self.registry)
        labels.labels('c', 'd').inc()

        self.gb.push(prefix='pre.fix')
        self.t.join()

        self.assertEqual(b'pre.fix.labels.a.c.b.d 1.0 1434898897\n', self.data)

    def test_prefix_tags(self):
        self._use_tags()
        labels = Gauge('labels', 'help', ['a', 'b'], registry=self.registry)
        labels.labels('c', 'd').inc()

        self.gb.push(prefix='pre.fix')
        self.t.join()

        self.assertEqual(b'pre.fix.labels;a=c;b=d 1.0 1434898897\n', self.data)

    def test_sanitizing(self):
        labels = Gauge('labels', 'help', ['a'], registry=self.registry)
        labels.labels('c.:8').inc()

        self.gb.push()
        self.t.join()

        self.assertEqual(b'labels.a.c__8 1.0 1434898897\n', self.data)

    def test_sanitizing_tags(self):
        self._use_tags()
        labels = Gauge('labels', 'help', ['a'], registry=self.registry)
        labels.labels('c.:8').inc()

        self.gb.push()
        self.t.join()

        self.assertEqual(b'labels;a=c__8 1.0 1434898897\n', self.data)
