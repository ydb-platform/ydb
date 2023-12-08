import unittest
from oauthlib.uri_validate import is_absolute_uri

from tests.unittest import TestCase


class UriValidateTest(TestCase):

    def test_is_absolute_uri(self):
        self.assertIsNotNone(is_absolute_uri('schema://example.com/path'))
        self.assertIsNotNone(is_absolute_uri('https://example.com/path'))
        self.assertIsNotNone(is_absolute_uri('https://example.com'))
        self.assertIsNotNone(is_absolute_uri('https://example.com:443/path'))
        self.assertIsNotNone(is_absolute_uri('https://example.com:443/'))
        self.assertIsNotNone(is_absolute_uri('https://example.com:443'))
        self.assertIsNotNone(is_absolute_uri('http://example.com'))
        self.assertIsNotNone(is_absolute_uri('http://example.com/path'))
        self.assertIsNotNone(is_absolute_uri('http://example.com:80/path'))

    def test_query(self):
        self.assertIsNotNone(is_absolute_uri('http://example.com:80/path?foo'))
        self.assertIsNotNone(is_absolute_uri('http://example.com:80/path?foo=bar'))
        self.assertIsNotNone(is_absolute_uri('http://example.com:80/path?foo=bar&fruit=banana'))

    def test_fragment_forbidden(self):
        self.assertIsNone(is_absolute_uri('http://example.com:80/path#foo'))
        self.assertIsNone(is_absolute_uri('http://example.com:80/path#foo=bar'))
        self.assertIsNone(is_absolute_uri('http://example.com:80/path#foo=bar&fruit=banana'))

    def test_combined_forbidden(self):
        self.assertIsNone(is_absolute_uri('http://example.com:80/path?foo#bar'))
        self.assertIsNone(is_absolute_uri('http://example.com:80/path?foo&bar#fruit'))
        self.assertIsNone(is_absolute_uri('http://example.com:80/path?foo=1&bar#fruit=banana'))
        self.assertIsNone(is_absolute_uri('http://example.com:80/path?foo=1&bar=2#fruit=banana&bar=foo'))

    def test_custom_scheme(self):
        self.assertIsNotNone(is_absolute_uri('com.example.bundle.id://'))

    def test_ipv6_bracket(self):
        self.assertIsNotNone(is_absolute_uri('http://[::1]:38432/path'))
        self.assertIsNotNone(is_absolute_uri('http://[::1]/path'))
        self.assertIsNotNone(is_absolute_uri('http://[fd01:0001::1]/path'))
        self.assertIsNotNone(is_absolute_uri('http://[fd01:1::1]/path'))
        self.assertIsNotNone(is_absolute_uri('http://[0123:4567:89ab:cdef:0123:4567:89ab:cdef]/path'))
        self.assertIsNotNone(is_absolute_uri('http://[0123:4567:89ab:cdef:0123:4567:89ab:cdef]:8080/path'))

    @unittest.skip("ipv6 edge-cases not supported")
    def test_ipv6_edge_cases(self):
        self.assertIsNotNone(is_absolute_uri('http://2001:db8::'))
        self.assertIsNotNone(is_absolute_uri('http://::1234:5678'))
        self.assertIsNotNone(is_absolute_uri('http://2001:db8::1234:5678'))
        self.assertIsNotNone(is_absolute_uri('http://2001:db8:3333:4444:5555:6666:7777:8888'))
        self.assertIsNotNone(is_absolute_uri('http://2001:db8:3333:4444:CCCC:DDDD:EEEE:FFFF'))
        self.assertIsNotNone(is_absolute_uri('http://0123:4567:89ab:cdef:0123:4567:89ab:cdef/path'))
        self.assertIsNotNone(is_absolute_uri('http://::'))
        self.assertIsNotNone(is_absolute_uri('http://2001:0db8:0001:0000:0000:0ab9:C0A8:0102'))

    @unittest.skip("ipv6 dual ipv4 not supported")
    def test_ipv6_dual(self):
        self.assertIsNotNone(is_absolute_uri('http://2001:db8:3333:4444:5555:6666:1.2.3.4'))
        self.assertIsNotNone(is_absolute_uri('http://::11.22.33.44'))
        self.assertIsNotNone(is_absolute_uri('http://2001:db8::123.123.123.123'))
        self.assertIsNotNone(is_absolute_uri('http://::1234:5678:91.123.4.56'))
        self.assertIsNotNone(is_absolute_uri('http://::1234:5678:1.2.3.4'))
        self.assertIsNotNone(is_absolute_uri('http://2001:db8::1234:5678:5.6.7.8'))

    def test_ipv4(self):
        self.assertIsNotNone(is_absolute_uri('http://127.0.0.1:38432/'))
        self.assertIsNotNone(is_absolute_uri('http://127.0.0.1:38432/'))
        self.assertIsNotNone(is_absolute_uri('http://127.1:38432/'))

    def test_failures(self):
        self.assertIsNone(is_absolute_uri('http://example.com:notaport/path'))
        self.assertIsNone(is_absolute_uri('wrong'))
        self.assertIsNone(is_absolute_uri('http://[:1]:38432/path'))
        self.assertIsNone(is_absolute_uri('http://[abcd:efgh::1]/'))

    def test_recursive_regex(self):
        from datetime import datetime
        t0 = datetime.now()
        is_absolute_uri('http://[::::::::::::::::::::::::::]/path')
        t1 = datetime.now()
        spent = t1 - t0
        self.assertGreater(0.1, spent.total_seconds(), "possible recursive loop detected")
