# -*- coding: utf-8 -*-
import requests
import unittest

from requests_toolbelt.cookies.forgetful import ForgetfulCookieJar
from . import get_betamax


class TestForgetfulCookieJar(unittest.TestCase):

    def setUp(self):
        self.session = requests.Session()
        self.session.cookies = ForgetfulCookieJar()
        self.recorder = get_betamax(self.session)

    def test_cookies_are_ignored(self):
        with self.recorder.use_cassette('http2bin_cookies'):
            url = 'https://httpbin.org/cookies/set'
            cookies = {
                'cookie0': 'value0',
            }
            r = self.session.request(
                'GET', url,
                 params=cookies
                )
            assert 'cookie0' not in self.session.cookies
