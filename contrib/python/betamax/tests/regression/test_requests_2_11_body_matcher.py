import os
import unittest

import pytest
import requests

from betamax import Betamax


class TestRequests211BodyMatcher(unittest.TestCase):
    def tearDown(self):
        os.unlink('tests/cassettes/requests_2_11_body_matcher.json')

    @pytest.mark.skipif(requests.__build__ < 0x020401,
                        reason="No json keyword.")
    def test_requests_with_json_body(self):
        s = requests.Session()
        with Betamax(s).use_cassette('requests_2_11_body_matcher',
                                     match_requests_on=['body']):
            r = s.post('https://httpbin.org/post', json={'a': 2})
            assert r.json() is not None

        s = requests.Session()
        with Betamax(s).use_cassette('requests_2_11_body_matcher',
                                     match_requests_on=['body']):
            r = s.post('https://httpbin.org/post', json={'a': 2})
            assert r.json() is not None
