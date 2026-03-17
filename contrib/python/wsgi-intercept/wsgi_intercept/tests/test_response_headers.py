"""Test response header validations.

Response headers are supposed to be bytestrings and some servers,
notably will experience an error if they are given headers with
the wrong form. Since wsgi-intercept is standing in as a server,
it should behave like one on this front. At the moment it does
not. There are tests for how it delivers request headers, but
not the other way round. Let's write some tests to fix that.
"""

import pytest
import requests
import six

import wsgi_intercept
from wsgi_intercept.interceptor import RequestsInterceptor


class HeaderApp(object):
    """A simple app that returns whatever headers we give it."""

    def __init__(self, headers):
        self.headers = headers

    def __call__(self, environ, start_response):

        headers = []
        for header in self.headers:
            headers.append((header, self.headers[header]))
        start_response('200 OK', headers)
        return [b'']


def app(headers):
    return HeaderApp(headers)


def test_header_app():
    """Make sure the header apps returns headers.

    Many libraries normalize headers to strings so we're not
    going to get exact matches.
    """
    header_value = 'alpha'
    header_value_str = 'alpha'

    def header_app():
        return app({'request-id': header_value})

    with RequestsInterceptor(header_app) as url:
        response = requests.get(url)

    assert response.headers['request-id'] == header_value_str


def test_encoding_violation():
    """If the header is unicode we expect boom."""
    header_key = 'request-id'
    if six.PY2:
        header_value = u'alpha'
    else:
        header_value = b'alpha'
    # we expect our http library to give us a str
    returned_header = 'alpha'

    def header_app():
        return app({header_key: header_value})

    # save original
    strict_response_headers = wsgi_intercept.STRICT_RESPONSE_HEADERS

    # With STRICT_RESPONSE_HEADERS True, response headers must be
    # native str.
    with RequestsInterceptor(header_app) as url:
        wsgi_intercept.STRICT_RESPONSE_HEADERS = True

        with pytest.raises(TypeError) as error:
            response = requests.get(url)

        assert (
            str(error.value) == "Header has a key '%s' or value '%s' "
            "which is not a native str." % (header_key, header_value))

        # When False, other types of strings are okay.
        wsgi_intercept.STRICT_RESPONSE_HEADERS = False

        response = requests.get(url)

        assert response.headers['request-id'] == returned_header

    # reset back to saved original
    wsgi_intercept.STRICT_RESPONSE_HEADERS = \
        strict_response_headers
