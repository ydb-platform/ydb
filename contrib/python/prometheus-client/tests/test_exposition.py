import gzip
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
import threading
import time
import unittest

import pytest

from prometheus_client import (
    CollectorRegistry, CONTENT_TYPE_LATEST, CONTENT_TYPE_PLAIN_0_0_4,
    CONTENT_TYPE_PLAIN_1_0_0, core, Counter, delete_from_gateway, Enum, Gauge,
    generate_latest, Histogram, Info, instance_ip_grouping_key, Metric,
    push_to_gateway, pushadd_to_gateway, Summary,
)
from prometheus_client.core import GaugeHistogramMetricFamily, Timestamp
from prometheus_client.exposition import (
    basic_auth_handler, choose_encoder, default_handler, MetricsHandler,
    passthrough_redirect_handler, tls_auth_handler,
)
import prometheus_client.openmetrics.exposition as openmetrics


class TestGenerateText(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()

        # Mock time so _created values are fixed.
        self.old_time = time.time
        time.time = lambda: 123.456

    def tearDown(self):
        time.time = self.old_time

    def custom_collector(self, metric_family):
        class CustomCollector:
            def collect(self):
                return [metric_family]

        self.registry.register(CustomCollector())

    def test_counter(self):
        c = Counter('cc', 'A counter', registry=self.registry)
        c.inc()
        self.assertEqual(b"""# HELP cc_total A counter
# TYPE cc_total counter
cc_total 1.0
# HELP cc_created A counter
# TYPE cc_created gauge
cc_created 123.456
""", generate_latest(self.registry, openmetrics.ALLOWUTF8))

    def test_counter_utf8(self):
        c = Counter('utf8.cc', 'A counter', registry=self.registry)
        c.inc()
        self.assertEqual(b"""# HELP "utf8.cc_total" A counter
# TYPE "utf8.cc_total" counter
{"utf8.cc_total"} 1.0
# HELP "utf8.cc_created" A counter
# TYPE "utf8.cc_created" gauge
{"utf8.cc_created"} 123.456
""", generate_latest(self.registry, openmetrics.ALLOWUTF8))

    def test_counter_utf8_escaped_underscores(self):
        c = Counter('utf8.cc', 'A counter', registry=self.registry)
        c.inc()
        assert b"""# HELP utf8_cc_total A counter
# TYPE utf8_cc_total counter
utf8_cc_total 1.0
# HELP utf8_cc_created A counter
# TYPE utf8_cc_created gauge
utf8_cc_created 123.456
""" == generate_latest(self.registry, openmetrics.UNDERSCORES)

    def test_counter_name_unit_append(self):
        c = Counter('requests', 'Request counter', unit="total", registry=self.registry)
        c.inc()
        self.assertEqual(b"""# HELP requests_total_total Request counter
# TYPE requests_total_total counter
requests_total_total 1.0
# HELP requests_total_created Request counter
# TYPE requests_total_created gauge
requests_total_created 123.456
""", generate_latest(self.registry))

    def test_counter_total(self):
        c = Counter('cc_total', 'A counter', registry=self.registry)
        c.inc()
        self.assertEqual(b"""# HELP cc_total A counter
# TYPE cc_total counter
cc_total 1.0
# HELP cc_created A counter
# TYPE cc_created gauge
cc_created 123.456
""", generate_latest(self.registry))

    def test_gauge(self):
        g = Gauge('gg', 'A gauge', registry=self.registry)
        g.set(17)
        self.assertEqual(b'# HELP gg A gauge\n# TYPE gg gauge\ngg 17.0\n', generate_latest(self.registry))

    def test_summary(self):
        s = Summary('ss', 'A summary', ['a', 'b'], registry=self.registry)
        s.labels('c', 'd').observe(17)
        self.assertEqual(b"""# HELP ss A summary
# TYPE ss summary
ss_count{a="c",b="d"} 1.0
ss_sum{a="c",b="d"} 17.0
# HELP ss_created A summary
# TYPE ss_created gauge
ss_created{a="c",b="d"} 123.456
""", generate_latest(self.registry))

    def test_histogram(self):
        s = Histogram('hh', 'A histogram', registry=self.registry)
        s.observe(0.05)
        self.assertEqual(b"""# HELP hh A histogram
# TYPE hh histogram
hh_bucket{le="0.005"} 0.0
hh_bucket{le="0.01"} 0.0
hh_bucket{le="0.025"} 0.0
hh_bucket{le="0.05"} 1.0
hh_bucket{le="0.075"} 1.0
hh_bucket{le="0.1"} 1.0
hh_bucket{le="0.25"} 1.0
hh_bucket{le="0.5"} 1.0
hh_bucket{le="0.75"} 1.0
hh_bucket{le="1.0"} 1.0
hh_bucket{le="2.5"} 1.0
hh_bucket{le="5.0"} 1.0
hh_bucket{le="7.5"} 1.0
hh_bucket{le="10.0"} 1.0
hh_bucket{le="+Inf"} 1.0
hh_count 1.0
hh_sum 0.05
# HELP hh_created A histogram
# TYPE hh_created gauge
hh_created 123.456
""", generate_latest(self.registry))

    def test_gaugehistogram(self):
        self.custom_collector(GaugeHistogramMetricFamily('gh', 'help', buckets=[('1.0', 4), ('+Inf', 5)], gsum_value=7))
        self.assertEqual(b"""# HELP gh help
# TYPE gh histogram
gh_bucket{le="1.0"} 4.0
gh_bucket{le="+Inf"} 5.0
# HELP gh_gcount help
# TYPE gh_gcount gauge
gh_gcount 5.0
# HELP gh_gsum help
# TYPE gh_gsum gauge
gh_gsum 7.0
""", generate_latest(self.registry))

    def test_info(self):
        i = Info('ii', 'A info', ['a', 'b'], registry=self.registry)
        i.labels('c', 'd').info({'foo': 'bar'})
        self.assertEqual(b'# HELP ii_info A info\n# TYPE ii_info gauge\nii_info{a="c",b="d",foo="bar"} 1.0\n',
                         generate_latest(self.registry))

    def test_enum(self):
        i = Enum('ee', 'An enum', ['a', 'b'], registry=self.registry, states=['foo', 'bar'])
        i.labels('c', 'd').state('bar')
        self.assertEqual(
            b'# HELP ee An enum\n# TYPE ee gauge\nee{a="c",b="d",ee="foo"} 0.0\nee{a="c",b="d",ee="bar"} 1.0\n',
            generate_latest(self.registry))

    def test_unicode(self):
        c = Gauge('cc', '\u4500', ['l'], registry=self.registry)
        c.labels('\u4500').inc()
        self.assertEqual(b'# HELP cc \xe4\x94\x80\n# TYPE cc gauge\ncc{l="\xe4\x94\x80"} 1.0\n',
                         generate_latest(self.registry))

    def test_escaping(self):
        g = Gauge('cc', 'A\ngaug\\e', ['a'], registry=self.registry)
        g.labels('\\x\n"').inc(1)
        self.assertEqual(b'# HELP cc A\\ngaug\\\\e\n# TYPE cc gauge\ncc{a="\\\\x\\n\\""} 1.0\n',
                         generate_latest(self.registry))

    def test_nonnumber(self):
        class MyNumber:
            def __repr__(self):
                return "MyNumber(123)"

            def __float__(self):
                return 123.0

        class MyCollector:
            def collect(self):
                metric = Metric("nonnumber", "Non number", 'untyped')
                metric.add_sample("nonnumber", {}, MyNumber())
                yield metric

        self.registry.register(MyCollector())
        self.assertEqual(b'# HELP nonnumber Non number\n# TYPE nonnumber untyped\nnonnumber 123.0\n',
                         generate_latest(self.registry))

    def test_timestamp(self):
        class MyCollector:
            def collect(self):
                metric = Metric("ts", "help", 'untyped')
                metric.add_sample("ts", {"foo": "a"}, 0, 123.456)
                metric.add_sample("ts", {"foo": "b"}, 0, -123.456)
                metric.add_sample("ts", {"foo": "c"}, 0, 123)
                metric.add_sample("ts", {"foo": "d"}, 0, Timestamp(123, 456000000))
                metric.add_sample("ts", {"foo": "e"}, 0, Timestamp(123, 456000))
                metric.add_sample("ts", {"foo": "f"}, 0, Timestamp(123, 456))
                yield metric

        self.registry.register(MyCollector())
        self.assertEqual(b"""# HELP ts help
# TYPE ts untyped
ts{foo="a"} 0.0 123456
ts{foo="b"} 0.0 -123456
ts{foo="c"} 0.0 123000
ts{foo="d"} 0.0 123456
ts{foo="e"} 0.0 123000
ts{foo="f"} 0.0 123000
""", generate_latest(self.registry))


class TestPushGateway(unittest.TestCase):
    def setUp(self):
        redirect_flag = 'testFlag'
        self.redirect_flag = redirect_flag  # preserve a copy for downstream test assertions
        self.registry = CollectorRegistry()
        self.counter = Gauge('g', 'help', registry=self.registry)
        self.requests = requests = []

        class TestHandler(BaseHTTPRequestHandler):
            def do_PUT(self):
                if 'with_basic_auth' in self.requestline and self.headers['authorization'] != 'Basic Zm9vOmJhcg==':
                    self.send_response(401)
                elif 'redirect' in self.requestline and redirect_flag not in self.requestline:
                    # checks for an initial test request with 'redirect' but without the redirect_flag,
                    # and simulates a redirect to a url with the redirect_flag (which will produce a 201)
                    self.send_response(301)
                    self.send_header('Location', getattr(self, 'redirect_address', None))
                else:
                    self.send_response(201)
                length = int(self.headers['content-length'])
                requests.append((self, self.rfile.read(length)))
                self.end_headers()

            do_POST = do_PUT
            do_DELETE = do_PUT

        # set up a separate server to serve a fake redirected request.
        # the redirected URL will have `redirect_flag` added to it,
        # which will cause the request handler to return 201.
        httpd_redirect = HTTPServer(('localhost', 0), TestHandler)
        self.redirect_address = TestHandler.redirect_address = \
            f'http://localhost:{httpd_redirect.server_address[1]}/{redirect_flag}'

        class TestRedirectServer(threading.Thread):
            def run(self):
                httpd_redirect.handle_request()

        self.redirect_server = TestRedirectServer()
        self.redirect_server.daemon = True
        self.redirect_server.start()

        # set up the normal server to serve the example requests across test cases.
        httpd = HTTPServer(('localhost', 0), TestHandler)
        self.address = f'http://localhost:{httpd.server_address[1]}'

        class TestServer(threading.Thread):
            def run(self):
                httpd.handle_request()

        self.server = TestServer()
        self.server.daemon = True
        self.server.start()


    def test_push(self):
        push_to_gateway(self.address, "my_job", self.registry)
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_schemeless_url(self):
        push_to_gateway(self.address.replace('http://', ''), "my_job", self.registry)
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_with_groupingkey(self):
        push_to_gateway(self.address, "my_job", self.registry, {'a': 9})
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job/a/9')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_with_groupingkey_empty_label(self):
        push_to_gateway(self.address, "my_job", self.registry, {'a': ''})
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job/a@base64/=')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_with_complex_groupingkey(self):
        push_to_gateway(self.address, "my_job", self.registry, {'a': 9, 'b': 'a/ z'})
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job/a/9/b@base64/YS8geg==')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_with_complex_job(self):
        push_to_gateway(self.address, "my/job", self.registry)
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job@base64/bXkvam9i')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_pushadd(self):
        pushadd_to_gateway(self.address, "my_job", self.registry)
        self.assertEqual(self.requests[0][0].command, 'POST')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_pushadd_with_groupingkey(self):
        pushadd_to_gateway(self.address, "my_job", self.registry, {'a': 9})
        self.assertEqual(self.requests[0][0].command, 'POST')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job/a/9')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_delete(self):
        delete_from_gateway(self.address, "my_job")
        self.assertEqual(self.requests[0][0].command, 'DELETE')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'')

    def test_delete_with_groupingkey(self):
        delete_from_gateway(self.address, "my_job", {'a': 9})
        self.assertEqual(self.requests[0][0].command, 'DELETE')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job/a/9')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'')

    def test_push_with_handler(self):
        def my_test_handler(url, method, timeout, headers, data):
            headers.append(['X-Test-Header', 'foobar'])
            # Handler should be passed sane default timeout
            self.assertEqual(timeout, 30)
            return default_handler(url, method, timeout, headers, data)

        push_to_gateway(self.address, "my_job", self.registry, handler=my_test_handler)
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][0].headers.get('x-test-header'), 'foobar')
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_with_basic_auth_handler(self):
        def my_auth_handler(url, method, timeout, headers, data):
            return basic_auth_handler(url, method, timeout, headers, data, "foo", "bar")

        push_to_gateway(self.address, "my_job_with_basic_auth", self.registry, handler=my_auth_handler)
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job_with_basic_auth')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_with_tls_auth_handler(self):
        def my_auth_handler(url, method, timeout, headers, data):
            import yatest.common as yc
            certs_dir = os.path.join(os.path.dirname(os.path.realpath(yc.source_path(__file__))), 'certs')
            return tls_auth_handler(url, method, timeout, headers, data, os.path.join(certs_dir, "cert.pem"), os.path.join(certs_dir, "key.pem"))

        push_to_gateway(self.address, "my_job_with_tls_auth", self.registry, handler=my_auth_handler)
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job_with_tls_auth')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_with_redirect_handler(self):
        def my_redirect_handler(url, method, timeout, headers, data):
            return passthrough_redirect_handler(url, method, timeout, headers, data)

        push_to_gateway(self.address, "my_job_with_redirect", self.registry, handler=my_redirect_handler)
        self.assertEqual(self.requests[0][0].command, 'PUT')
        self.assertEqual(self.requests[0][0].path, '/metrics/job/my_job_with_redirect')
        self.assertEqual(self.requests[0][0].headers.get('content-type'), CONTENT_TYPE_PLAIN_0_0_4)
        self.assertEqual(self.requests[0][1], b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

        # ensure the redirect preserved request settings from the initial request.
        self.assertEqual(self.requests[0][0].command, self.requests[1][0].command)
        self.assertEqual(
            self.requests[0][0].headers.get('content-type'),
            self.requests[1][0].headers.get('content-type')
        )
        self.assertEqual(self.requests[0][1], self.requests[1][1])

        # ensure the redirect took place at the expected redirect location.
        self.assertEqual(self.requests[1][0].path, "/" + self.redirect_flag)

    def test_push_with_trailing_slash(self):
        address = self.address + '/'
        push_to_gateway(address, "my_job_with_trailing_slash", self.registry)

        self.assertNotIn('//', self.requests[0][0].path)

    def test_push_with_gzip_compression(self):
        push_to_gateway(self.address, "my_job", self.registry, compression='gzip')
        request, body = self.requests[0]
        self.assertEqual(request.headers.get('content-encoding'), 'gzip')
        decompressed = gzip.decompress(body)
        self.assertEqual(decompressed, b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_with_snappy_compression(self):
        snappy = pytest.importorskip('snappy')
        push_to_gateway(self.address, "my_job", self.registry, compression='snappy')
        request, body = self.requests[0]
        self.assertEqual(request.headers.get('content-encoding'), 'snappy')
        decompressor = snappy.StreamDecompressor()
        decompressed = decompressor.decompress(body)
        flush = getattr(decompressor, 'flush', None)
        if callable(flush):
            decompressed += flush()
        self.assertEqual(decompressed, b'# HELP g help\n# TYPE g gauge\ng 0.0\n')

    def test_push_with_invalid_compression(self):
        with self.assertRaisesRegex(ValueError, 'Unsupported compression type'):
            push_to_gateway(self.address, "my_job", self.registry, compression='brotli')
        self.assertEqual(self.requests, [])

    def test_instance_ip_grouping_key(self):
        self.assertTrue('' != instance_ip_grouping_key()['instance'])

    def test_metrics_handler(self):
        handler = MetricsHandler.factory(self.registry)
        self.assertEqual(handler.registry, self.registry)

    def test_metrics_handler_subclassing(self):
        subclass = type('MetricsHandlerSubclass', (MetricsHandler, object), {})
        handler = subclass.factory(self.registry)

        self.assertTrue(issubclass(handler, (MetricsHandler, subclass)))


@pytest.fixture
def registry():
    return core.CollectorRegistry()


class Collector:
    def __init__(self, metric_family, *values):
        self.metric_family = metric_family
        self.values = values

    def collect(self):
        self.metric_family.add_metric([], *self.values)
        return [self.metric_family]


def _expect_metric_exception(registry, expected_error):
    try:
        generate_latest(registry, openmetrics.ALLOWUTF8)
    except expected_error as exception:
        assert isinstance(exception.args[-1], core.Metric)
        # Got a valid error as expected, return quietly
        return

    raise RuntimeError('Expected exception not raised')


@pytest.mark.parametrize('MetricFamily', [
    core.CounterMetricFamily,
    core.GaugeMetricFamily,
])
@pytest.mark.parametrize('value,error', [
    (None, TypeError),
    ('', ValueError),
    ('x', ValueError),
    ([], TypeError),
    ({}, TypeError),
])
def test_basic_metric_families(registry, MetricFamily, value, error):
    metric_family = MetricFamily(MetricFamily.__name__, 'help')
    registry.register(Collector(metric_family, value))
    _expect_metric_exception(registry, error)


@pytest.mark.parametrize('count_value,sum_value,error', [
    (None, 0, TypeError),
    (0, None, TypeError),
    ('', 0, ValueError),
    (0, '', ValueError),
    ([], 0, TypeError),
    (0, [], TypeError),
    ({}, 0, TypeError),
    (0, {}, TypeError),
])
def test_summary_metric_family(registry, count_value, sum_value, error):
    metric_family = core.SummaryMetricFamily('summary', 'help')
    registry.register(Collector(metric_family, count_value, sum_value))
    _expect_metric_exception(registry, error)


@pytest.mark.parametrize('MetricFamily', [
    core.GaugeHistogramMetricFamily,
])
@pytest.mark.parametrize('buckets,sum_value,error', [
    ([('spam', 0), ('eggs', 0)], None, TypeError),
    ([('spam', 0), ('eggs', None)], 0, TypeError),
    ([('spam', 0), (None, 0)], 0, AttributeError),
    ([('spam', None), ('eggs', 0)], 0, TypeError),
    ([(None, 0), ('eggs', 0)], 0, AttributeError),
    ([('spam', 0), ('eggs', 0)], '', ValueError),
    ([('spam', 0), ('eggs', '')], 0, ValueError),
    ([('spam', ''), ('eggs', 0)], 0, ValueError),
])
def test_histogram_metric_families(MetricFamily, registry, buckets, sum_value, error):
    metric_family = MetricFamily(MetricFamily.__name__, 'help')
    registry.register(Collector(metric_family, buckets, sum_value))
    _expect_metric_exception(registry, error)


class TestChooseEncoder(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()
        c = Counter('dotted.counter', 'A counter', registry=self.registry)
        c.inc()

    def custom_collector(self, metric_family):
        class CustomCollector:
            def collect(self):
                return [metric_family]

        self.registry.register(CustomCollector())

    def assert_is_escaped(self, exp):
        self.assertRegex(exp, r'.*\ndotted_counter_total 1.0\n.*')

    def assert_is_utf8(self, exp):
        self.assertRegex(exp, r'.*\n{"dotted.counter_total"} 1.0\n.*')

    def assert_is_prom(self, exp):
        self.assertNotRegex(exp, r'# EOF')

    def assert_is_openmetrics(self, exp):
        self.assertRegex(exp, r'# EOF')

    def test_default_encoder(self):
        generator, content_type = choose_encoder(None)
        assert content_type == CONTENT_TYPE_PLAIN_0_0_4
        exp = generator(self.registry).decode('utf-8')
        self.assert_is_escaped(exp)
        self.assert_is_prom(exp)

    def test_plain_encoder(self):
        generator, content_type = choose_encoder(CONTENT_TYPE_PLAIN_0_0_4)
        assert content_type == CONTENT_TYPE_PLAIN_0_0_4
        exp = generator(self.registry).decode('utf-8')
        self.assert_is_escaped(exp)
        self.assert_is_prom(exp)

    def test_openmetrics_latest(self):
        generator, content_type = choose_encoder(openmetrics.CONTENT_TYPE_LATEST)
        assert content_type == 'application/openmetrics-text; version=1.0.0; charset=utf-8; escaping=underscores'
        exp = generator(self.registry).decode('utf-8')
        self.assert_is_escaped(exp)
        self.assert_is_openmetrics(exp)

    def test_openmetrics_utf8(self):
        generator, content_type = choose_encoder(openmetrics.CONTENT_TYPE_LATEST + '; escaping=allow-utf-8')
        assert content_type == openmetrics.CONTENT_TYPE_LATEST + '; escaping=allow-utf-8'
        exp = generator(self.registry).decode('utf-8')
        self.assert_is_utf8(exp)
        self.assert_is_openmetrics(exp)

    def test_openmetrics_dots_escaping(self):
        generator, content_type = choose_encoder(openmetrics.CONTENT_TYPE_LATEST + '; escaping=dots')
        assert content_type == openmetrics.CONTENT_TYPE_LATEST + '; escaping=dots'
        exp = generator(self.registry).decode('utf-8')
        self.assertRegex(exp, r'.*\ndotted_dot_counter__total 1.0\n.*')
        self.assert_is_openmetrics(exp)

    def test_prom_latest(self):
        generator, content_type = choose_encoder(CONTENT_TYPE_LATEST)
        assert content_type == CONTENT_TYPE_PLAIN_1_0_0 + '; escaping=underscores'
        exp = generator(self.registry).decode('utf-8')
        self.assert_is_escaped(exp)
        self.assert_is_prom(exp)

    def test_prom_plain_1_0_0(self):
        generator, content_type = choose_encoder(CONTENT_TYPE_PLAIN_1_0_0)
        assert content_type == CONTENT_TYPE_PLAIN_1_0_0 + '; escaping=underscores'
        exp = generator(self.registry).decode('utf-8')
        self.assert_is_escaped(exp)
        self.assert_is_prom(exp)

    def test_prom_utf8(self):
        generator, content_type = choose_encoder(CONTENT_TYPE_PLAIN_1_0_0 + '; escaping=allow-utf-8')
        assert content_type == CONTENT_TYPE_PLAIN_1_0_0 + '; escaping=allow-utf-8'
        exp = generator(self.registry).decode('utf-8')
        self.assert_is_utf8(exp)
        self.assert_is_prom(exp)

    def test_prom_dots_escaping(self):
        generator, content_type = choose_encoder(CONTENT_TYPE_PLAIN_1_0_0 + '; escaping=dots')
        assert content_type == CONTENT_TYPE_PLAIN_1_0_0 + '; escaping=dots'
        exp = generator(self.registry).decode('utf-8')
        self.assertRegex(exp, r'.*\ndotted_dot_counter__total 1.0\n.*')
        self.assert_is_prom(exp)

    def test_openmetrics_no_version(self):
        generator, content_type = choose_encoder('application/openmetrics-text; charset=utf-8; escaping=allow-utf-8')
        assert content_type == 'application/openmetrics-text; version=1.0.0; charset=utf-8'
        exp = generator(self.registry).decode('utf-8')
        # No version -- allow-utf-8 rejected.
        self.assert_is_escaped(exp)
        self.assert_is_openmetrics(exp)

    def test_prom_no_version(self):
        generator, content_type = choose_encoder('text/plain; charset=utf-8; escaping=allow-utf-8')
        assert content_type == 'text/plain; version=0.0.4; charset=utf-8'
        exp = generator(self.registry).decode('utf-8')
        # No version -- allow-utf-8 rejected.
        self.assert_is_escaped(exp)
        self.assert_is_prom(exp)


@pytest.mark.parametrize("scenario", [
    {
        "name": "empty string",
        "input": "",
        "expectedUnderscores": "",
        "expectedDots": "",
        "expectedValue": "",
    },
    {
        "name": "legacy valid metric name",
        "input": "no:escaping_required",
        "expectedUnderscores": "no:escaping_required",
        "expectedDots": "no:escaping__required",
        "expectedValue": "no:escaping_required",
    },
    {
        "name": "metric name with dots",
        "input": "mysystem.prod.west.cpu.load",
        "expectedUnderscores": "mysystem_prod_west_cpu_load",
        "expectedDots": "mysystem_dot_prod_dot_west_dot_cpu_dot_load",
        "expectedValue": "U__mysystem_2e_prod_2e_west_2e_cpu_2e_load",
    },
    {
        "name": "metric name with dots and underscore",
        "input": "mysystem.prod.west.cpu.load_total",
        "expectedUnderscores": "mysystem_prod_west_cpu_load_total",
        "expectedDots": "mysystem_dot_prod_dot_west_dot_cpu_dot_load__total",
        "expectedValue": "U__mysystem_2e_prod_2e_west_2e_cpu_2e_load__total",
    },
    {
        "name": "metric name with dots and colon",
        "input": "http.status:sum",
        "expectedUnderscores": "http_status:sum",
        "expectedDots": "http_dot_status:sum",
        "expectedValue": "U__http_2e_status:sum",
    },
    {
        "name": "metric name with spaces and emoji",
        "input": "label with 😱",
        "expectedUnderscores": "label_with__",
        "expectedDots": "label__with____",
        "expectedValue": "U__label_20_with_20__1f631_",
    },
    {
        "name": "metric name with unicode characters > 0x100",
        "input": "花火",
        "expectedUnderscores": "__",
        "expectedDots": "____",
        "expectedValue": "U___82b1__706b_",
    },
    {
        "name": "metric name with spaces and edge-case value",
        "input": "label with \u0100",
        "expectedUnderscores": "label_with__",
        "expectedDots": "label__with____",
        "expectedValue": "U__label_20_with_20__100_",
    },
])
def test_escape_metric_name(scenario):
    input = scenario["input"]

    got = openmetrics.escape_metric_name(input, openmetrics.UNDERSCORES)
    assert got == scenario["expectedUnderscores"], f"[{scenario['name']}] Underscore escaping failed"

    got = openmetrics.escape_metric_name(input, openmetrics.DOTS)
    assert got == scenario["expectedDots"], f"[{scenario['name']}] Dots escaping failed"

    got = openmetrics.escape_metric_name(input, openmetrics.VALUES)
    assert got == scenario["expectedValue"], f"[{scenario['name']}] Value encoding failed"


@pytest.mark.parametrize("scenario", [
    {
        "name": "empty string",
        "input": "",
        "expectedUnderscores": "",
        "expectedDots": "",
        "expectedValue": "",
    },
    {
        "name": "legacy valid label name",
        "input": "no_escaping_required",
        "expectedUnderscores": "no_escaping_required",
        "expectedDots": "no__escaping__required",
        "expectedValue": "no_escaping_required",
    },
    {
        "name": "label name with dots",
        "input": "mysystem.prod.west.cpu.load",
        "expectedUnderscores": "mysystem_prod_west_cpu_load",
        "expectedDots": "mysystem_dot_prod_dot_west_dot_cpu_dot_load",
        "expectedValue": "U__mysystem_2e_prod_2e_west_2e_cpu_2e_load",
    },
    {
        "name": "label name with dots and underscore",
        "input": "mysystem.prod.west.cpu.load_total",
        "expectedUnderscores": "mysystem_prod_west_cpu_load_total",
        "expectedDots": "mysystem_dot_prod_dot_west_dot_cpu_dot_load__total",
        "expectedValue": "U__mysystem_2e_prod_2e_west_2e_cpu_2e_load__total",
    },
    {
        "name": "label name with dots and colon",
        "input": "http.status:sum",
        "expectedUnderscores": "http_status_sum",
        "expectedDots": "http_dot_status__sum",
        "expectedValue": "U__http_2e_status_3a_sum",
    },
    {
        "name": "label name with spaces and emoji",
        "input": "label with 😱",
        "expectedUnderscores": "label_with__",
        "expectedDots": "label__with____",
        "expectedValue": "U__label_20_with_20__1f631_",
    },
    {
        "name": "label name with unicode characters > 0x100",
        "input": "花火",
        "expectedUnderscores": "__",
        "expectedDots": "____",
        "expectedValue": "U___82b1__706b_",
    },
    {
        "name": "label name with spaces and edge-case value",
        "input": "label with \u0100",
        "expectedUnderscores": "label_with__",
        "expectedDots": "label__with____",
        "expectedValue": "U__label_20_with_20__100_",
    },
])
def test_escape_label_name(scenario):
    input = scenario["input"]

    got = openmetrics.escape_label_name(input, openmetrics.UNDERSCORES)
    assert got == scenario["expectedUnderscores"], f"[{scenario['name']}] Underscore escaping failed"

    got = openmetrics.escape_label_name(input, openmetrics.DOTS)
    assert got == scenario["expectedDots"], f"[{scenario['name']}] Dots escaping failed"

    got = openmetrics.escape_label_name(input, openmetrics.VALUES)
    assert got == scenario["expectedValue"], f"[{scenario['name']}] Value encoding failed"


if __name__ == '__main__':
    unittest.main()
