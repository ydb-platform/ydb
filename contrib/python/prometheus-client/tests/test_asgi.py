import asyncio
import gzip
from unittest import TestCase

from asgiref.testing import ApplicationCommunicator

from prometheus_client import CollectorRegistry, Counter, make_asgi_app
from prometheus_client.exposition import CONTENT_TYPE_PLAIN_0_0_4


def setup_testing_defaults(scope):
    scope.update(
        {
            "client": ("127.0.0.1", 32767),
            "headers": [],
            "http_version": "1.0",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "scheme": "http",
            "server": ("127.0.0.1", 80),
            "type": "http",
        }
    )


class ASGITest(TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()
        self.captured_status = None
        self.captured_headers = None
        # Setup ASGI scope
        self.scope = {}
        setup_testing_defaults(self.scope)
        self.communicator = None

    def tearDown(self):
        if self.communicator:
            asyncio.new_event_loop().run_until_complete(
                self.communicator.wait()
            )
            
    def seed_app(self, app):
        self.communicator = ApplicationCommunicator(app, self.scope)

    def send_input(self, payload):
        asyncio.new_event_loop().run_until_complete(
            self.communicator.send_input(payload)
        )

    def send_default_request(self):
        self.send_input({"type": "http.request", "body": b""})

    def get_output(self):
        output = asyncio.new_event_loop().run_until_complete(
            self.communicator.receive_output(0)
        )
        return output

    def get_all_output(self):
        outputs = []
        while True:
            try:
                outputs.append(self.get_output())
            except asyncio.TimeoutError:
                break
        return outputs

    def get_all_response_headers(self):
        outputs = self.get_all_output()
        response_start = next(o for o in outputs if o["type"] == "http.response.start")
        return response_start["headers"]

    def get_response_header_value(self, header_name):
        response_headers = self.get_all_response_headers()
        return next(
            value.decode("utf-8")
            for name, value in response_headers
            if name.decode("utf-8") == header_name
        )

    def increment_metrics(self, metric_name, help_text, increments):
        c = Counter(metric_name, help_text, registry=self.registry)
        for _ in range(increments):
            c.inc()

    def assert_metrics(self, output, metric_name, help_text, increments):
        self.assertIn("# HELP " + metric_name + "_total " + help_text + "\n", output)
        self.assertIn("# TYPE " + metric_name + "_total counter\n", output)
        self.assertIn(metric_name + "_total " + str(increments) + ".0\n", output)

    def assert_not_metrics(self, output, metric_name, help_text, increments):
        self.assertNotIn("# HELP " + metric_name + "_total " + help_text + "\n", output)
        self.assertNotIn("# TYPE " + metric_name + "_total counter\n", output)
        self.assertNotIn(metric_name + "_total " + str(increments) + ".0\n", output)

    def assert_outputs(self, outputs, metric_name, help_text, increments, compressed):
        self.assertEqual(len(outputs), 2)
        response_start = outputs[0]
        self.assertEqual(response_start['type'], 'http.response.start')
        response_body = outputs[1]
        self.assertEqual(response_body['type'], 'http.response.body')
        # Status code
        self.assertEqual(response_start['status'], 200)
        # Headers
        num_of_headers = 2 if compressed else 1
        self.assertEqual(len(response_start['headers']), num_of_headers)
        self.assertIn((b"Content-Type", CONTENT_TYPE_PLAIN_0_0_4.encode('utf8')), response_start['headers'])
        if compressed:
            self.assertIn((b"Content-Encoding", b"gzip"), response_start['headers'])
        # Body
        if compressed:
            output = gzip.decompress(response_body['body']).decode('utf8')
        else:
            output = response_body['body'].decode('utf8')

        self.assert_metrics(output, metric_name, help_text, increments)

    def validate_metrics(self, metric_name, help_text, increments):
        """
        ASGI app serves the metrics from the provided registry.
        """
        self.increment_metrics(metric_name, help_text, increments)
        # Create and run ASGI app
        app = make_asgi_app(self.registry)
        self.seed_app(app)
        self.send_default_request()
        # Assert outputs
        outputs = self.get_all_output()
        self.assert_outputs(outputs, metric_name, help_text, increments, compressed=False)

    def test_report_metrics_1(self):
        self.validate_metrics("counter", "A counter", 2)

    def test_report_metrics_2(self):
        self.validate_metrics("counter", "Another counter", 3)

    def test_report_metrics_3(self):
        self.validate_metrics("requests", "Number of requests", 5)

    def test_report_metrics_4(self):
        self.validate_metrics("failed_requests", "Number of failed requests", 7)

    def test_gzip(self):
        # Increment a metric.
        metric_name = "counter"
        help_text = "A counter"
        increments = 2
        self.increment_metrics(metric_name, help_text, increments)
        app = make_asgi_app(self.registry)
        self.seed_app(app)
        # Send input with gzip header.
        self.scope["headers"] = [(b"accept-encoding", b"gzip")]
        self.send_input({"type": "http.request", "body": b""})
        # Assert outputs are compressed.
        outputs = self.get_all_output()
        self.assert_outputs(outputs, metric_name, help_text, increments, compressed=True)

    def test_gzip_disabled(self):
        # Increment a metric.
        metric_name = "counter"
        help_text = "A counter"
        increments = 2
        self.increment_metrics(metric_name, help_text, increments)
        # Disable compression explicitly.
        app = make_asgi_app(self.registry, disable_compression=True)
        self.seed_app(app)
        # Send input with gzip header.
        self.scope["headers"] = [(b"accept-encoding", b"gzip")]
        self.send_input({"type": "http.request", "body": b""})
        # Assert outputs are not compressed.
        outputs = self.get_all_output()
        self.assert_outputs(outputs, metric_name, help_text, increments, compressed=False)

    def test_openmetrics_encoding(self):
        """Response content type is application/openmetrics-text when appropriate Accept header is in request"""
        app = make_asgi_app(self.registry)
        self.seed_app(app)
        self.scope["headers"] = [(b"Accept", b"application/openmetrics-text; version=1.0.0")]
        self.send_input({"type": "http.request", "body": b""})

        content_type = self.get_response_header_value('Content-Type').split(";")[0]
        assert content_type == "application/openmetrics-text"

    def test_plaintext_encoding(self):
        """Response content type is text/plain when Accept header is missing in request"""
        app = make_asgi_app(self.registry)
        self.seed_app(app)
        self.send_input({"type": "http.request", "body": b""})

        content_type = self.get_response_header_value('Content-Type').split(";")[0]
        assert content_type == "text/plain"

    def test_qs_parsing(self):
        """Only metrics that match the 'name[]' query string param appear"""

        app = make_asgi_app(self.registry)
        metrics = [
            ("asdf", "first test metric", 1),
            ("bsdf", "second test metric", 2)
        ]

        for m in metrics:
            self.increment_metrics(*m)

        for i_1 in range(len(metrics)):
            self.seed_app(app)
            self.scope['query_string'] = f"name[]={metrics[i_1][0]}_total".encode("utf-8")
            self.send_default_request()

            outputs = self.get_all_output()
            response_body = outputs[1]
            output = response_body['body'].decode('utf8')

            self.assert_metrics(output, *metrics[i_1])

            for i_2 in range(len(metrics)):
                if i_1 == i_2:
                    continue

                self.assert_not_metrics(output, *metrics[i_2])

            asyncio.new_event_loop().run_until_complete(
                self.communicator.wait()
            )
