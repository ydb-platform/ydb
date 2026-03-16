# -*- coding: utf-8 -*-
try:
    from unittest import mock
except ImportError:
    import mock
import pytest
import requests
import unittest

from requests_toolbelt import SSLAdapter
from . import get_betamax


class TestSSLAdapter(unittest.TestCase):
    def setUp(self):
        self.session = requests.Session()
        self.session.mount('https://', SSLAdapter('SSLv3'))
        self.recorder = get_betamax(self.session)

    def test_klevas(self):
        with self.recorder.use_cassette('klevas_vu_lt_ssl3'):
            r = self.session.get('https://klevas.vu.lt/')
            assert r.status_code == 200

    @pytest.mark.skipif(requests.__build__ < 0x020400,
                        reason="Requires Requests v2.4.0 or later")
    @mock.patch('requests.packages.urllib3.poolmanager.ProxyManager')
    def test_proxies(self, ProxyManager):
        a = SSLAdapter('SSLv3')
        a.proxy_manager_for('http://127.0.0.1:8888')

        assert ProxyManager.call_count == 1
        kwargs = ProxyManager.call_args_list[0][1]
        assert kwargs['ssl_version'] == 'SSLv3'
