import email
import os
import unittest
from datetime import datetime

import pytest

from betamax import __version__
from betamax.cassette import cassette
from betamax import mock_response
from betamax import recorder
from betamax import serializers
from betamax import util
from requests.models import Response, Request
from requests.packages import urllib3
from requests.structures import CaseInsensitiveDict

try:
    from requests.packages.urllib3._collections import HTTPHeaderDict
except ImportError:
    from betamax.headers import HTTPHeaderDict


def decode(s):
    if hasattr(s, 'decode'):
        return s.decode()
    return s


class Serializer(serializers.BaseSerializer):
    name = 'test'

    @staticmethod
    def generate_cassette_name(cassette_library_dir, cassette_name):
        return 'test_cassette.test'

    def on_init(self):
        self.serialize_calls = []
        self.deserialize_calls = []

    def serialize(self, data):
        self.serialize_calls.append(data)
        return ''

    def deserialize(self, data):
        self.deserialize_calls.append(data)
        return {}


class TestSerialization(unittest.TestCase):

    """Unittests for the serialization and deserialization functions.

    This tests:

        - deserialize_prepared_request
        - deserialize_response
        - serialize_prepared_request
        - serialize_response

    """

    def test_serialize_response(self):
        r = Response()
        r.status_code = 200
        r.reason = 'OK'
        r.encoding = 'utf-8'
        r.headers = CaseInsensitiveDict()
        r.url = 'http://example.com'
        util.add_urllib3_response({
            'body': {
                'string': decode('foo'),
                'encoding': 'utf-8'
            }
        }, r, HTTPHeaderDict())
        serialized = util.serialize_response(r, False)
        assert serialized is not None
        assert serialized != {}
        assert serialized['body']['encoding'] == 'utf-8'
        assert serialized['body']['string'] == 'foo'
        assert serialized['headers'] == {}
        assert serialized['url'] == 'http://example.com'
        assert serialized['status'] == {'code': 200, 'message': 'OK'}

    def test_deserialize_response_old(self):
        """For the previous version of Betamax and backwards compatibility."""
        s = {
            'body': {
                'string': decode('foo'),
                'encoding': 'utf-8'
            },
            'headers': {
                'Content-Type': decode('application/json')
            },
            'url': 'http://example.com/',
            'status_code': 200,
            'recorded_at': '2013-08-31T00:00:01'
        }
        r = util.deserialize_response(s)
        assert r.content == b'foo'
        assert r.encoding == 'utf-8'
        assert r.headers == {'Content-Type': 'application/json'}
        assert r.url == 'http://example.com/'
        assert r.status_code == 200

    def test_deserialize_response_new(self):
        """This adheres to the correct cassette specification."""
        s = {
            'body': {
                'string': decode('foo'),
                'encoding': 'utf-8'
            },
            'headers': {
                'Content-Type': [decode('application/json')]
            },
            'url': 'http://example.com/',
            'status': {'code': 200, 'message': 'OK'},
            'recorded_at': '2013-08-31T00:00:01'
        }
        r = util.deserialize_response(s)
        assert r.content == b'foo'
        assert r.encoding == 'utf-8'
        assert r.headers == {'Content-Type': 'application/json'}
        assert r.url == 'http://example.com/'
        assert r.status_code == 200
        assert r.reason == 'OK'

    def test_serialize_prepared_request(self):
        r = Request()
        r.method = 'GET'
        r.url = 'http://example.com'
        r.headers = {'User-Agent': 'betamax/test header'}
        r.data = {'key': 'value'}
        p = r.prepare()
        serialized = util.serialize_prepared_request(p, False)
        assert serialized is not None
        assert serialized != {}
        assert serialized['method'] == 'GET'
        assert serialized['uri'] == 'http://example.com/'
        assert serialized['headers'] == {
            'Content-Length': ['9'],
            'Content-Type': ['application/x-www-form-urlencoded'],
            'User-Agent': ['betamax/test header'],
        }
        assert serialized['body']['string'] == 'key=value'

    def test_deserialize_prepared_request(self):
        s = {
            'body': 'key=value',
            'headers': {
                'User-Agent': 'betamax/test header',
            },
            'method': 'GET',
            'uri': 'http://example.com/',
        }
        p = util.deserialize_prepared_request(s)
        assert p.body == 'key=value'
        assert p.headers == CaseInsensitiveDict(
            {'User-Agent': 'betamax/test header'}
        )
        assert p.method == 'GET'
        assert p.url == 'http://example.com/'

    def test_from_list_returns_an_element(self):
        a = ['value']
        assert util.from_list(a) == 'value'

    def test_from_list_handles_non_lists(self):
        a = 'value'
        assert util.from_list(a) == 'value'

    def test_add_urllib3_response(self):
        r = Response()
        r.status_code = 200
        r.headers = {}
        util.add_urllib3_response({
            'body': {
                'string': decode('foo'),
                'encoding': 'utf-8'
            }
        }, r, HTTPHeaderDict())
        assert isinstance(r.raw, urllib3.response.HTTPResponse)
        assert r.content == b'foo'
        assert isinstance(r.raw._original_response,
                          mock_response.MockHTTPResponse)


def test_cassette_initialization():
    serializers.serializer_registry['test'] = Serializer()
    cassette.Cassette.default_cassette_options['placeholders'] = []

    with recorder.Betamax.configure() as config:
        config.define_cassette_placeholder('<TO-OVERRIDE>', 'default')
        config.define_cassette_placeholder('<KEEP-DEFAULT>', 'config')
        placeholders = [{
            'placeholder': '<TO-OVERRIDE>',
            'replace': 'override',
        }, {
            'placeholder': '<ONLY-OVERRIDE>',
            'replace': 'cassette',
        }]
        instance = cassette.Cassette(
            'test_cassette',
            'test',
            placeholders=placeholders
        )

        expected = [
            cassette.Placeholder('<TO-OVERRIDE>', 'override'),
            cassette.Placeholder('<KEEP-DEFAULT>', 'config'),
            cassette.Placeholder('<ONLY-OVERRIDE>', 'cassette'),
        ]
        assert instance.placeholders == expected

    cassette.Cassette.default_cassette_options['placeholders'] = []


class TestCassette(unittest.TestCase):
    cassette_name = 'test_cassette'

    def setUp(self):
        # Make a new serializer to test with
        self.test_serializer = Serializer()
        serializers.serializer_registry['test'] = self.test_serializer

        # Instantiate the cassette to test with
        self.cassette = cassette.Cassette(
            TestCassette.cassette_name,
            'test',
            record_mode='once'
        )

        # Create a new object to serialize
        r = Response()
        r.status_code = 200
        r.reason = 'OK'
        r.encoding = 'utf-8'
        r.headers = CaseInsensitiveDict({'Content-Type': decode('foo')})
        r.url = 'http://example.com'
        util.add_urllib3_response({
            'body': {
                'string': decode('foo'),
                'encoding': 'utf-8'
            }
        }, r, HTTPHeaderDict({'Content-Type': decode('foo')}))
        self.response = r

        # Create an associated request
        r = Request()
        r.method = 'GET'
        r.url = 'http://example.com'
        r.headers = {}
        r.data = {'key': 'value'}
        self.response.request = r.prepare()
        self.response.request.headers.update(
            {'User-Agent': 'betamax/test header'}
        )

        # Expected serialized cassette data.
        self.json = {
            'request': {
                'body': {
                    'encoding': 'utf-8',
                    'string': 'key=value',
                },
                'headers': {
                    'User-Agent': ['betamax/test header'],
                    'Content-Length': ['9'],
                    'Content-Type': ['application/x-www-form-urlencoded'],
                },
                'method': 'GET',
                'uri': 'http://example.com/',
            },
            'response': {
                'body': {
                    'string': decode('foo'),
                    'encoding': 'utf-8',
                },
                'headers': {'Content-Type': [decode('foo')]},
                'status': {'code': 200, 'message': 'OK'},
                'url': 'http://example.com',
            },
            'recorded_at': '2013-08-31T00:00:00',
        }
        self.date = datetime(2013, 8, 31)
        self.cassette.save_interaction(self.response, self.response.request)
        self.interaction = self.cassette.interactions[0]

    def tearDown(self):
        try:
            self.cassette.eject()
        except:
            pass
        if os.path.exists(TestCassette.cassette_name):
            os.unlink(TestCassette.cassette_name)

    def test_serialize_interaction(self):
        serialized = self.interaction.data
        assert serialized['request'] == self.json['request']
        assert serialized['response'] == self.json['response']
        assert serialized.get('recorded_at') is not None

    def test_holds_interactions(self):
        assert isinstance(self.cassette.interactions, list)
        assert self.cassette.interactions != []
        assert self.interaction in self.cassette.interactions

    def test_find_match(self):
        self.cassette.match_options = set(['uri', 'method'])
        self.cassette.record_mode = 'none'
        i = self.cassette.find_match(self.response.request)
        assert i is not None
        assert self.interaction is i

    def test_find_match_new_episodes_with_existing_unused_interactions(self):
        """See bug 153 in GitHub for details.

        https://github.com/betamaxpy/betamax/issues/153
        """
        self.cassette.match_options = set(['method', 'uri'])
        self.cassette.record_mode = 'new_episodes'
        i = self.cassette.find_match(self.response.request)
        assert i is not None
        assert self.interaction is i

    def test_find_match_new_episodes_with_no_unused_interactions(self):
        """See bug 153 in GitHub for details.

        https://github.com/betamaxpy/betamax/issues/153
        """
        self.cassette.match_options = set(['method', 'uri'])
        self.cassette.record_mode = 'new_episodes'
        self.interaction.used = True
        i = self.cassette.find_match(self.response.request)
        assert i is None

    def test_find_match__missing_matcher(self):
        self.cassette.match_options = set(['uri', 'method', 'invalid'])
        self.cassette.record_mode = 'none'
        with pytest.raises(KeyError):
            self.cassette.find_match(self.response.request)

    def test_eject(self):
        serializer = self.test_serializer
        self.cassette.eject()
        assert serializer.serialize_calls == [
            {'http_interactions': [self.cassette.interactions[0].data],
             'recorded_with': 'betamax/{0}'.format(__version__)}
            ]

    def test_earliest_recorded_date(self):
        assert self.interaction.recorded_at is not None
        assert self.cassette.earliest_recorded_date is not None


class TestInteraction(unittest.TestCase):
    def setUp(self):
        self.request = {
            'body': {
                'string': 'key=value&key2=secret_value',
                'encoding': 'utf-8'
            },
            'headers': {
                'User-Agent': ['betamax/test header'],
                'Content-Length': ['9'],
                'Content-Type': ['application/x-www-form-urlencoded'],
                'Authorization': ['123456789abcdef'],
                },
            'method': 'GET',
            'uri': 'http://example.com/',
        }
        self.response = {
            'body': {
                'string': decode('foo'),
                'encoding': 'utf-8'
            },
            'headers': {
                'Content-Type': [decode('foo')],
                'Set-Cookie': ['cookie_name=cookie_value',
                               'sessionid=deadbeef']
            },
            'status_code': 200,
            'url': 'http://example.com',
        }
        self.json = {
            'request': self.request,
            'response': self.response,
            'recorded_at': '2013-08-31T00:00:00',
        }
        self.interaction = cassette.Interaction(self.json)
        self.date = datetime(2013, 8, 31)

    def test_as_response(self):
        r = self.interaction.as_response()
        assert isinstance(r, Response)

    def test_as_response_returns_new_instance(self):
        r1 = self.interaction.as_response()
        r2 = self.interaction.as_response()
        assert r1 is not r2

    def test_deserialized_response(self):
        def check_uri(attr):
            # Necessary since PreparedRequests do not have a uri attr
            if attr == 'uri':
                return 'url'
            return attr
        r = self.interaction.as_response()
        for attr in ['status_code', 'url']:
            assert self.response[attr] == decode(getattr(r, attr))

        headers = dict((k, ', '.join(v))
                       for k, v in self.response['headers'].items())
        assert headers == r.headers

        tested_cookie = False
        for cookie in r.cookies:
            cookie_str = "{0}={1}".format(cookie.name, cookie.value)
            assert cookie_str in r.headers['Set-Cookie']
            tested_cookie = True
        assert tested_cookie

        assert self.response['body']['string'] == decode(r.content)
        actual_req = r.request
        expected_req = self.request
        for attr in ['method', 'uri']:
            assert expected_req[attr] == getattr(actual_req, check_uri(attr))

        assert self.request['body']['string'] == decode(actual_req.body)
        headers = dict((k, v[0]) for k, v in expected_req['headers'].items())
        assert headers == actual_req.headers
        assert self.date == self.interaction.recorded_at

    def test_match(self):
        matchers = [lambda x: True, lambda x: False, lambda x: True]
        assert self.interaction.match(matchers) is False
        matchers[1] = lambda x: True
        assert self.interaction.match(matchers) is True

    def test_replace(self):
        self.interaction.replace('123456789abcdef', '<AUTH_TOKEN>')
        self.interaction.replace('cookie_value', '<COOKIE_VALUE>')
        self.interaction.replace('secret_value', '<SECRET_VALUE>')
        self.interaction.replace('foo', '<FOO>')
        self.interaction.replace('http://example.com', '<EXAMPLE_URI>')
        self.interaction.replace('', '<IF_FAIL_THIS_INSERTS_BEFORE_AND_AFTER_EACH_CHARACTER')

        header = (
            self.interaction.data['request']['headers']['Authorization'][0])
        assert header == '<AUTH_TOKEN>'
        header = self.interaction.data['response']['headers']['Set-Cookie']
        assert header[0] == 'cookie_name=<COOKIE_VALUE>'
        assert header[1] == 'sessionid=deadbeef'
        body = self.interaction.data['request']['body']['string']
        assert body == 'key=value&key2=<SECRET_VALUE>'
        body = self.interaction.data['response']['body']
        assert body == {'encoding': 'utf-8', 'string': '<FOO>'}
        uri = self.interaction.data['request']['uri']
        assert uri == '<EXAMPLE_URI>/'
        uri = self.interaction.data['response']['url']
        assert uri == '<EXAMPLE_URI>'

    def test_replace_in_headers(self):
        self.interaction.replace_in_headers('123456789abcdef', '<AUTH_TOKEN>')
        self.interaction.replace_in_headers('cookie_value', '<COOKIE_VALUE>')
        header = (
            self.interaction.data['request']['headers']['Authorization'][0])
        assert header == '<AUTH_TOKEN>'
        header = self.interaction.data['response']['headers']['Set-Cookie'][0]
        assert header == 'cookie_name=<COOKIE_VALUE>'

    def test_replace_in_body(self):
        self.interaction.replace_in_body('secret_value', '<SECRET_VALUE>')
        self.interaction.replace_in_body('foo', '<FOO>')
        body = self.interaction.data['request']['body']['string']
        assert body == 'key=value&key2=<SECRET_VALUE>'
        body = self.interaction.data['response']['body']
        assert body == {'encoding': 'utf-8', 'string': '<FOO>'}

    def test_replace_in_uri(self):
        self.interaction.replace_in_uri('http://example.com', '<EXAMPLE_URI>')
        uri = self.interaction.data['request']['uri']
        assert uri == '<EXAMPLE_URI>/'
        uri = self.interaction.data['response']['url']
        assert uri == '<EXAMPLE_URI>'


class TestMockHTTPResponse(unittest.TestCase):
    def setUp(self):
        self.resp = mock_response.MockHTTPResponse(HTTPHeaderDict({
            decode('Header'): decode('value')
        }))

    def test_isclosed(self):
        assert self.resp.isclosed() is False

    def test_is_Message(self):
        assert isinstance(self.resp.msg, email.message.Message)

    def test_close(self):
        self.resp.close()
        assert self.resp.isclosed() is True
