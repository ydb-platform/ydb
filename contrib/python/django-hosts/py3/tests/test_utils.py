from django_hosts.utils import normalize_scheme, normalize_port

from .base import HostsTestCase


class UtilsTest(HostsTestCase):

    def test_normalize_scheme(self):
        self.assertEqual(normalize_scheme('http'), 'http://')
        self.assertEqual(normalize_scheme('http:'), 'http://')
        self.assertEqual(normalize_scheme(), '//')

    def test_normalize_port(self):
        self.assertEqual(normalize_port(None), '')
        self.assertEqual(normalize_port(':80:'), ':80')
        self.assertEqual(normalize_port('80'), ':80')
        self.assertEqual(normalize_port('80:'), ':80')
        self.assertEqual(normalize_port(), '')
