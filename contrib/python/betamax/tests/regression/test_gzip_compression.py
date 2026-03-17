import os
import unittest

from betamax import Betamax
from requests import Session


class TestGZIPRegression(unittest.TestCase):
    def tearDown(self):
        os.unlink('tests/cassettes/gzip_regression.json')

    def test_saves_content_as_gzip(self):
        s = Session()
        with Betamax(s).use_cassette('gzip_regression'):
            r = s.get(
                'https://api.github.com/repos/github3py/fork_this/issues/1',
                headers={'Accept-Encoding': 'gzip, deflate, compress'}
                )
            assert r.headers.get('Content-Encoding') == 'gzip'
            assert r.json() is not None

            r2 = s.get(
                'https://api.github.com/repos/github3py/fork_this/issues/1',
                headers={'Accept-Encoding': 'gzip, deflate, compress'}
                )
            assert r2.headers.get('Content-Encoding') == 'gzip'
            assert r2.json() is not None
            assert r2.json() == r.json()

        s = Session()
        with Betamax(s).use_cassette('gzip_regression'):
            r = s.get(
                'https://api.github.com/repos/github3py/fork_this/issues/1'
                )
            assert r.json() is not None
