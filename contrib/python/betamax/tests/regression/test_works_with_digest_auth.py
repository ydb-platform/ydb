import unittest

from betamax import Betamax
from requests import Session
from requests.auth import HTTPDigestAuth


class TestDigestAuth(unittest.TestCase):
    def test_saves_content_as_gzip(self):
        s = Session()
        cassette_name = 'handles_digest_auth'
        match = ['method', 'uri', 'digest-auth']
        with Betamax(s).use_cassette(cassette_name, match_requests_on=match):
            r = s.get('https://httpbin.org/digest-auth/auth/user/passwd',
                      auth=HTTPDigestAuth('user', 'passwd'))
            assert r.ok
            assert r.history[0].status_code == 401

        s = Session()
        with Betamax(s).use_cassette(cassette_name, match_requests_on=match):
            r = s.get('https://httpbin.org/digest-auth/auth/user/passwd',
                      auth=HTTPDigestAuth('user', 'passwd'))
            assert r.json() is not None
