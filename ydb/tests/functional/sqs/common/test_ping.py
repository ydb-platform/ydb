#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests

from hamcrest import assert_that, equal_to

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase


class TestPing(KikimrSqsTestBase):
    def test_ping(self):
        result = requests.get('http://localhost:{}/private/ping'.format(self.sqs_port))
        assert_that(result.status_code, equal_to(200))
        assert_that(result.text, equal_to('pong'))

        result = requests.get('http://localhost:{}/private/ping/'.format(self.sqs_port))
        assert_that(result.status_code, equal_to(200))
        assert_that(result.text, equal_to('pong'))

    def test_error_on_cgi_parameters(self):
        result = requests.get('http://localhost:{}/private/ping?cgi=1'.format(self.sqs_port))
        assert_that(result.status_code, equal_to(400))

    def test_error_on_non_ping_path(self):
        result = requests.get('http://localhost:{}/private/ping/a'.format(self.sqs_port))
        assert_that(result.status_code, equal_to(400))

        result = requests.get('http://localhost:{}/private/a/ping'.format(self.sqs_port))
        assert_that(result.status_code, equal_to(400))

        result = requests.get('http://localhost:{}/a/private/ping'.format(self.sqs_port))
        assert_that(result.status_code, equal_to(400))
