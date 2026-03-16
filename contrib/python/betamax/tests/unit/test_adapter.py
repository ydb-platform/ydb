import unittest

try:
    from unittest import mock
except ImportError:
    import mock

from betamax.adapter import BetamaxAdapter
from requests.adapters import HTTPAdapter

from yatest import common


class TestBetamaxAdapter(unittest.TestCase):
    def setUp(self):
        http_adapter = mock.Mock()
        self.adapters_dict = {'http://': http_adapter}
        self.adapter = BetamaxAdapter(old_adapters=self.adapters_dict)

    def tearDown(self):
        self.adapter.eject_cassette()

    def test_has_http_adatper(self):
        assert self.adapter.http_adapter is not None
        assert isinstance(self.adapter.http_adapter, HTTPAdapter)

    def test_empty_initial_state(self):
        assert self.adapter.cassette is None
        assert self.adapter.cassette_name is None
        assert self.adapter.serialize is None

    def test_load_cassette(self):
        filename = 'test'
        self.adapter.load_cassette(filename, 'json', {
            'record': 'none',
            'cassette_library_dir': common.source_path("contrib/python/betamax/tests/cassettes/"),
        })
        assert self.adapter.cassette is not None
        assert self.adapter.cassette_name == filename
