from betamax import Betamax, BetamaxError
from requests import Session

import unittest


class TestReplays(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def test_replays_response_on_right_order(self):
        s = self.session
        opts = {'record': 'none'}
        with Betamax(s).use_cassette('test_replays_response_on_right_order', **opts) as betamax:
            self.cassette_path = betamax.current_cassette.cassette_path
            r0 = s.get('http://httpbin.org/get')
            r1 = s.get('http://httpbin.org/get')
            r0_found = (b'72.160.214.132' in r0.content)
            assert r0_found == True
            r1_found = (b'72.160.214.133' in r1.content)
            assert r1_found == True
