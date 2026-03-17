# -*- coding: utf-8 -*-
import requests
import unittest
try:
    from unittest import mock
except ImportError:
    import mock

from requests_toolbelt.auth.guess import GuessAuth, GuessProxyAuth
from . import get_betamax


class TestGuessAuth(unittest.TestCase):
    def setUp(self):
        self.session = requests.Session()
        self.recorder = get_betamax(self.session)

    def cassette(self, name):
        return self.recorder.use_cassette(
            'httpbin_guess_auth_' + name,
            match_requests_on=['method', 'uri', 'digest-auth']
        )

    def test_basic(self):
        with self.cassette('basic'):
            r = self.session.request(
                'GET', 'http://httpbin.org/basic-auth/user/passwd',
                auth=GuessAuth('user', 'passwd'))

        assert r.json() == {'authenticated': True, 'user': 'user'}

    def test_digest(self):
        with self.cassette('digest'):
            r = self.session.request(
                'GET', 'http://httpbin.org/digest-auth/auth/user/passwd',
                auth=GuessAuth('user', 'passwd'))

        assert r.json() == {'authenticated': True, 'user': 'user'}

    def test_no_auth(self):
        with self.cassette('none'):
            url = 'http://httpbin.org/get?a=1'
            r = self.session.request('GET', url,
                                     auth=GuessAuth('user', 'passwd'))

            j = r.json()
            assert j['args'] == {'a': '1'}
            assert j['url'] == url
            assert 'user' not in r.text
            assert 'passwd' not in r.text


class TestGuessProxyAuth(unittest.TestCase):

    @mock.patch('requests_toolbelt.auth.http_proxy_digest.HTTPProxyDigestAuth.handle_407')
    def test_handle_407_header_digest(self, mock_handle_407):
        r = requests.Response()
        r.headers['Proxy-Authenticate'] = 'Digest nonce="d2b19757d3d656a283c99762cbd1097b", opaque="1c311ad1cc6e6183b83bc75f95a57893", realm="me@kennethreitz.com", qop=auth'

        guess_auth = GuessProxyAuth(None, None, "user", "passwd")
        guess_auth.handle_407(r)

        mock_handle_407.assert_called_with(r)

    @mock.patch('requests.auth.HTTPProxyAuth.__call__')
    @mock.patch('requests.cookies.extract_cookies_to_jar')
    def test_handle_407_header_basic(self, extract_cookies_to_jar, proxy_auth_call):
        req = mock.Mock()
        r = mock.Mock()
        r.headers = dict()
        r.request.copy.return_value = req

        proxy_auth_call.return_value = requests.Response()

        kwargs = {}
        r.headers['Proxy-Authenticate'] = 'Basic realm="Fake Realm"'
        guess_auth = GuessProxyAuth(None, None, "user", "passwd")
        guess_auth.handle_407(r, *kwargs)

        proxy_auth_call.assert_called_with(req)
