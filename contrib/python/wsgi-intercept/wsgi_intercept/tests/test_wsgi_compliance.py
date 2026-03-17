import sys
import pytest
try:
    from urllib.parse import unquote
except ImportError:
    from urllib import unquote
from wsgi_intercept import httplib2_intercept
from . import wsgi_app
from .install import installer_class
import httplib2

HOST = 'some_hopefully_nonexistant_domain'

InstalledApp = installer_class(httplib2_intercept)


def test_simple_override():
    with InstalledApp(wsgi_app.simple_app, host=HOST) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain:80/', 'GET')
        assert app.success()


def test_simple_override_default_port():
    with InstalledApp(wsgi_app.simple_app, host=HOST) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain/', 'GET')
        assert app.success()


def test_https_in_environ():
    with InstalledApp(wsgi_app.simple_app, host=HOST, port=443) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'https://some_hopefully_nonexistant_domain/', 'GET')
        assert app.success()
        internal_env = app.get_internals()
        assert internal_env['wsgi.url_scheme'] == 'https'


def test_more_interesting():
    expected_uri = ('/%E4%B8%96%E4%B8%8A%E5%8E%9F%E4%BE%86%E9%82%84%E6'
                    '%9C%89%E3%80%8C%E7%BE%9A%E7%89%9B%E3%80%8D%E9%80%99'
                    '%E7%A8%AE%E5%8B%95%E7%89%A9%EF%BC%81%2Fbarney'
                    '?bar=baz%20zoom')
    with InstalledApp(wsgi_app.more_interesting_app, host=HOST) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain' + expected_uri,
            'GET',
            headers={'Accept': 'application/json',
                     'Cookie': 'foo=bar'})
        internal_env = app.get_internals()

        expected_path_info = unquote(expected_uri.split('?')[0])
        assert internal_env['REQUEST_URI'] == expected_uri
        assert internal_env['RAW_URI'] == expected_uri
        assert internal_env['QUERY_STRING'] == 'bar=baz%20zoom'
        assert internal_env['HTTP_ACCEPT'] == 'application/json'
        assert internal_env['HTTP_COOKIE'] == 'foo=bar'
        # In this test we are ensuring the value, in the environ, of
        # a request header has a value which is a str, as native to
        # that version of Python. That means always a str, despite
        # the fact that a str in Python 2 and 3 are different
        # things! PEP 3333 requires this. isinstance is not used to
        # avoid confusion over class hierarchies.
        assert type(internal_env['HTTP_COOKIE']) == type('') # noqa E721

        # Do the rather painful wsgi encoding dance.
        if sys.version_info[0] > 2:
            assert internal_env['PATH_INFO'].encode('latin-1').decode(
                'UTF-8') == expected_path_info
        else:
            assert internal_env['PATH_INFO'].decode(
                'UTF-8') == expected_path_info.decode('UTF-8')


def test_script_name():
    with InstalledApp(wsgi_app.more_interesting_app, host=HOST,
                      script_name='/funky') as app:
        http = httplib2.Http()
        response, content = http.request(
            'http://some_hopefully_nonexistant_domain/funky/boom/baz')
        internal_env = app.get_internals()

        assert internal_env['SCRIPT_NAME'] == '/funky'
        assert internal_env['PATH_INFO'] == '/boom/baz'


def test_encoding_errors():
    with InstalledApp(wsgi_app.more_interesting_app, host=HOST):
        http = httplib2.Http()
        with pytest.raises(UnicodeEncodeError):
            response, content = http.request(
                'http://some_hopefully_nonexistant_domain/boom/baz',
                headers={'Accept': u'application/\u2603'})


def test_post_status_headers():
    with InstalledApp(wsgi_app.post_status_headers_app, host=HOST) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain/', 'GET')
        assert app.success()
        assert resp.get('content-type') == 'text/plain'


def test_empty_iterator():
    with InstalledApp(wsgi_app.empty_string_app, host=HOST) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain/', 'GET')
        assert app.success()
        assert content == b'second'


def test_generator():
    with InstalledApp(wsgi_app.generator_app, host=HOST) as app:
        http = httplib2.Http()
        resp, content = http.request(
            'http://some_hopefully_nonexistant_domain/', 'GET')
        assert app.success()
        assert resp.get('content-type') == 'text/plain'
        assert content == b'First generated line\nSecond generated line\n'
