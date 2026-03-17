# -*- coding: utf-8 -*-
import unittest
import pytest

from requests_toolbelt import sessions
from requests import Request
from . import get_betamax


class TestBasedSession(unittest.TestCase):
    def test_request_with_base(self):
        session = sessions.BaseUrlSession('https://httpbin.org/')
        recorder = get_betamax(session)
        with recorder.use_cassette('simple_get_request'):
            response = session.get('/get')
        response.raise_for_status()

    def test_request_without_base(self):
        session = sessions.BaseUrlSession()
        with pytest.raises(ValueError):
            session.get('/')

    def test_request_override_base(self):
        session = sessions.BaseUrlSession('https://www.google.com')
        recorder = get_betamax(session)
        with recorder.use_cassette('simple_get_request'):
            response = session.get('https://httpbin.org/get')
        response.raise_for_status()
        assert response.json()['headers']['Host'] == 'httpbin.org'

    def test_prepared_request_with_base(self):
        session = sessions.BaseUrlSession('https://httpbin.org')
        request = Request(method="GET", url="/get")
        prepared_request = session.prepare_request(request)
        recorder = get_betamax(session)
        with recorder.use_cassette('simple_get_request'):
            response = session.send(prepared_request)
        response.raise_for_status()

    def test_prepared_request_without_base(self):
        session = sessions.BaseUrlSession()
        request = Request(method="GET", url="/")
        with pytest.raises(ValueError):
            prepared_request = session.prepare_request(request)
            session.send(prepared_request)

    def test_prepared_request_override_base(self):
        session = sessions.BaseUrlSession('https://www.google.com')
        request = Request(method="GET", url="https://httpbin.org/get")
        prepared_request = session.prepare_request(request)
        recorder = get_betamax(session)
        with recorder.use_cassette('simple_get_request'):
            response = session.send(prepared_request)
        response.raise_for_status()
        assert response.json()['headers']['Host'] == 'httpbin.org'
