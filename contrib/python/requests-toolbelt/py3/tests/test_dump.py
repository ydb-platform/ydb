"""Collection of tests for utils.dump.

The dump utility module only has two public attributes:

- dump_response
- dump_all

This module, however, tests many of the private implementation details since
those public functions just wrap them and testing the public functions will be
very complex and high-level.
"""
from requests_toolbelt._compat import HTTPHeaderDict
from requests_toolbelt.utils import dump

try:
    from unittest import mock
except ImportError:
    import mock
import pytest
import requests

from . import get_betamax

HTTP_1_1 = 11
HTTP_1_0 = 10
HTTP_0_9 = 9
HTTP_UNKNOWN = 5000


class TestSimplePrivateFunctions(object):

    """Excercise simple private functions in one logical place."""

    def test_coerce_to_bytes_skips_byte_strings(self):
        """Show that _coerce_to_bytes skips bytes input."""
        bytestr = b'some bytes'
        assert dump._coerce_to_bytes(bytestr) is bytestr

    def test_coerce_to_bytes_converts_text(self):
        """Show that _coerce_to_bytes handles text input."""
        bytestr = b'some bytes'
        text = bytestr.decode('utf-8')
        assert dump._coerce_to_bytes(text) == bytestr

    def test_format_header(self):
        """Prove that _format_header correctly formats bytes input."""
        header = b'Connection'
        value = b'close'
        expected = b'Connection: close\r\n'
        assert dump._format_header(header, value) == expected

    def test_format_header_handles_unicode(self):
        """Prove that _format_header correctly formats text input."""
        header = b'Connection'.decode('utf-8')
        value = b'close'.decode('utf-8')
        expected = b'Connection: close\r\n'
        assert dump._format_header(header, value) == expected

    def test_build_request_path(self):
        """Show we get the right request path for a normal request."""
        path, _ = dump._build_request_path(
            'https://example.com/foo/bar', {}
        )
        assert path == b'/foo/bar'

    def test_build_request_path_with_query_string(self):
        """Show we include query strings appropriately."""
        path, _ = dump._build_request_path(
            'https://example.com/foo/bar?query=data', {}
        )
        assert path == b'/foo/bar?query=data'

    def test_build_request_path_with_proxy_info(self):
        """Show that we defer to the proxy request_path info."""
        path, _ = dump._build_request_path(
            'https://example.com/', {
                'request_path': b'https://example.com/test'
            }
        )
        assert path == b'https://example.com/test'


class RequestResponseMixin(object):

    """Mix-in for test classes needing mocked requests and responses."""

    response_spec = [
        'connection',
        'content',
        'raw',
        'reason',
        'request',
        'url',
    ]

    request_spec = [
        'body',
        'headers',
        'method',
        'url',
    ]

    httpresponse_spec = [
        'headers',
        'reason',
        'status',
        'version',
    ]

    adapter_spec = [
        'proxy_manager',
    ]

    @pytest.fixture(autouse=True)
    def set_up(self):
        """xUnit style autoused fixture creating mocks."""
        self.response = mock.Mock(spec=self.response_spec)
        self.request = mock.Mock(spec=self.request_spec)
        self.httpresponse = mock.Mock(spec=self.httpresponse_spec)
        self.adapter = mock.Mock(spec=self.adapter_spec)

        self.response.connection = self.adapter
        self.response.request = self.request
        self.response.raw = self.httpresponse

    def configure_response(self, content=b'', proxy_manager=None, url=None,
                           reason=b''):
        """Helper function to configure a mocked response."""
        self.adapter.proxy_manager = proxy_manager or {}
        self.response.content = content
        self.response.url = url
        self.response.reason = reason

    def configure_request(self, body=b'', headers=None, method=None,
                          url=None):
        """Helper function to configure a mocked request."""
        self.request.body = body
        self.request.headers = headers or {}
        self.request.method = method
        self.request.url = url

    def configure_httpresponse(self, headers=None, reason=b'', status=200,
                               version=HTTP_1_1):
        """Helper function to configure a mocked urllib3 response."""
        self.httpresponse.headers = HTTPHeaderDict(headers or {})
        self.httpresponse.reason = reason
        self.httpresponse.status = status
        self.httpresponse.version = version


class TestResponsePrivateFunctions(RequestResponseMixin):

    """Excercise private functions using responses."""

    def test_get_proxy_information_sans_proxy(self):
        """Show no information is returned when not using a proxy."""
        self.configure_response()

        assert dump._get_proxy_information(self.response) is None

    def test_get_proxy_information_with_proxy_over_http(self):
        """Show only the request path is returned for HTTP requests.

        Using HTTP over a proxy doesn't alter anything except the request path
        of the request. The method doesn't change a dictionary with the
        request_path is the only thing that should be returned.
        """
        self.configure_response(
            proxy_manager={'http://': 'http://local.proxy:3939'},
        )
        self.configure_request(
            url='http://example.com',
            method='GET',
        )

        assert dump._get_proxy_information(self.response) == {
            'request_path': 'http://example.com'
        }

    def test_get_proxy_information_with_proxy_over_https(self):
        """Show that the request path and method are returned for HTTPS reqs.

        Using HTTPS over a proxy changes the method used and the request path.
        """
        self.configure_response(
            proxy_manager={'http://': 'http://local.proxy:3939'},
        )
        self.configure_request(
            url='https://example.com',
            method='GET',
        )

        assert dump._get_proxy_information(self.response) == {
            'method': 'CONNECT',
            'request_path': 'https://example.com'
        }

    def test_dump_request_data(self):
        """Build up the request data into a bytearray."""
        self.configure_request(
            url='http://example.com/',
            method='GET',
        )

        array = bytearray()
        prefixes = dump.PrefixSettings('request:', 'response:')
        dump._dump_request_data(
            request=self.request,
            prefixes=prefixes,
            bytearr=array,
            proxy_info={},
        )

        assert b'request:GET / HTTP/1.1\r\n' in array
        assert b'request:Host: example.com\r\n' in array

    def test_dump_non_string_request_data(self):
        """Build up the request data into a bytearray."""
        self.configure_request(
            url='http://example.com/',
            method='POST',
            body=1
        )

        array = bytearray()
        prefixes = dump.PrefixSettings('request:', 'response:')
        dump._dump_request_data(
            request=self.request,
            prefixes=prefixes,
            bytearr=array,
            proxy_info={},
        )
        assert b'request:POST / HTTP/1.1\r\n' in array
        assert b'request:Host: example.com\r\n' in array
        assert b'<< Request body is not a string-like type >>\r\n' in array

    def test_dump_request_data_with_proxy_info(self):
        """Build up the request data into a bytearray."""
        self.configure_request(
            url='http://example.com/',
            method='GET',
        )

        array = bytearray()
        prefixes = dump.PrefixSettings('request:', 'response:')
        dump._dump_request_data(
            request=self.request,
            prefixes=prefixes,
            bytearr=array,
            proxy_info={
                'request_path': b'fake-request-path',
                'method': b'CONNECT',
            },
        )

        assert b'request:CONNECT fake-request-path HTTP/1.1\r\n' in array
        assert b'request:Host: example.com\r\n' in array

    def test_dump_response_data(self):
        """Build up the response data into a bytearray."""
        self.configure_response(
            url='https://example.com/redirected',
            content=b'foobarbogus',
            reason=b'OK',
        )
        self.configure_httpresponse(
            headers={'Content-Type': 'application/json'},
            reason=b'OK',
            status=201,
        )

        array = bytearray()
        prefixes = dump.PrefixSettings('request:', 'response:')
        dump._dump_response_data(
            response=self.response,
            prefixes=prefixes,
            bytearr=array,
        )

        assert b'response:HTTP/1.1 201 OK\r\n' in array
        assert b'response:Content-Type: application/json\r\n' in array

    def test_dump_response_data_with_older_http_version(self):
        """Build up the response data into a bytearray."""
        self.configure_response(
            url='https://example.com/redirected',
            content=b'foobarbogus',
            reason=b'OK',
        )
        self.configure_httpresponse(
            headers={'Content-Type': 'application/json'},
            reason=b'OK',
            status=201,
            version=HTTP_0_9,
        )

        array = bytearray()
        prefixes = dump.PrefixSettings('request:', 'response:')
        dump._dump_response_data(
            response=self.response,
            prefixes=prefixes,
            bytearr=array,
        )

        assert b'response:HTTP/0.9 201 OK\r\n' in array
        assert b'response:Content-Type: application/json\r\n' in array

    def test_dump_response_data_with_unknown_http_version(self):
        """Build up the response data into a bytearray."""
        self.configure_response(
            url='https://example.com/redirected',
            content=b'foobarbogus',
            reason=b'OK',
        )
        self.configure_httpresponse(
            headers={'Content-Type': 'application/json'},
            reason=b'OK',
            status=201,
            version=HTTP_UNKNOWN,
        )

        array = bytearray()
        prefixes = dump.PrefixSettings('request:', 'response:')
        dump._dump_response_data(
            response=self.response,
            prefixes=prefixes,
            bytearr=array,
        )

        assert b'response:HTTP/? 201 OK\r\n' in array
        assert b'response:Content-Type: application/json\r\n' in array


class TestResponsePublicFunctions(RequestResponseMixin):

    """Excercise public functions using responses."""

    def test_dump_response_fails_without_request(self):
        """Show that a response without a request raises a ValueError."""
        del self.response.request
        assert hasattr(self.response, 'request') is False

        with pytest.raises(ValueError):
            dump.dump_response(self.response)

    def test_dump_response_uses_provided_bytearray(self):
        """Show that users providing bytearrays receive those back."""
        self.configure_request(
            url='http://example.com/',
            method='GET',
        )
        self.configure_response(
            url='https://example.com/redirected',
            content=b'foobarbogus',
            reason=b'OK',
        )
        self.configure_httpresponse(
            headers={'Content-Type': 'application/json'},
            reason=b'OK',
            status=201,
        )
        arr = bytearray()

        retarr = dump.dump_response(self.response, data_array=arr)
        assert retarr is arr


class TestDumpRealResponses(object):

    """Exercise dump utilities against real data."""

    def test_dump_response(self):
        session = requests.Session()
        recorder = get_betamax(session)
        with recorder.use_cassette('simple_get_request'):
            response = session.get('https://httpbin.org/get')

        arr = dump.dump_response(response)
        assert b'< GET /get HTTP/1.1\r\n' in arr
        assert b'< Host: httpbin.org\r\n' in arr
        # NOTE(sigmavirus24): The ? below is only because Betamax doesn't
        # preserve which HTTP version the server reports as supporting.
        # When not using Betamax, there should be a different version
        # reported.
        assert b'> HTTP/? 200 OK\r\n' in arr
        assert b'> Content-Type: application/json\r\n' in arr

    def test_dump_all(self):
        session = requests.Session()
        recorder = get_betamax(session)
        with recorder.use_cassette('redirect_request_for_dump_all'):
            response = session.get('https://httpbin.org/redirect/5')

        arr = dump.dump_all(response)
        assert b'< GET /redirect/5 HTTP/1.1\r\n' in arr
        assert b'> Location: /relative-redirect/4\r\n' in arr
        assert b'< GET /relative-redirect/4 HTTP/1.1\r\n' in arr
        assert b'> Location: /relative-redirect/3\r\n' in arr
        assert b'< GET /relative-redirect/3 HTTP/1.1\r\n' in arr
        assert b'> Location: /relative-redirect/2\r\n' in arr
        assert b'< GET /relative-redirect/2 HTTP/1.1\r\n' in arr
        assert b'> Location: /relative-redirect/1\r\n' in arr
        assert b'< GET /relative-redirect/1 HTTP/1.1\r\n' in arr
        assert b'> Location: /get\r\n' in arr
        assert b'< GET /get HTTP/1.1\r\n' in arr
