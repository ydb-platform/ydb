# -*- coding: utf-8 -*-
import requests
import unittest

from requests_toolbelt.adapters.fingerprint import FingerprintAdapter
from . import get_betamax


class TestFingerprintAdapter(unittest.TestCase):
    HTTP2BIN_FINGERPRINT = 'abf8683eeba8521ad2e8dc48e92a1cbea3ff8608f1417948fdad75d7b50eb264'

    def setUp(self):
        self.session = requests.Session()
        self.session.mount('https://http2bin.org', FingerprintAdapter(self.HTTP2BIN_FINGERPRINT))
        self.recorder = get_betamax(self.session)

    def test_fingerprint(self):
        with self.recorder.use_cassette('http2bin_fingerprint'):
            r = self.session.get('https://http2bin.org/get')
            assert r.status_code == 200
