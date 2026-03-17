"""
Simple WSGI applications for testing.
"""

from pprint import pformat

try:
    bytes
except ImportError:
    bytes = str


def simple_app(environ, start_response):
    """Simplest possible application object"""
    status = '200 OK'
    response_headers = [('Content-type', 'text/plain')]
    start_response(status, response_headers)
    return [b'WSGI intercept successful!\n']


def more_interesting_app(environ, start_response):
    start_response('200 OK', [('Content-type', 'text/plain')])
    return [pformat(environ).encode('utf-8')]


def post_status_headers_app(environ, start_response):
    headers = []
    start_response('200 OK', headers)
    headers.append(('Content-type', 'text/plain'))
    return [b'WSGI intercept successful!\n']


def raises_app(environ, start_response):
    raise TypeError("bah")


def empty_string_app(environ, start_response):
    start_response('200 OK', [('Content-type', 'text/plain')])
    return [b'', b'second']


def generator_app(environ, start_response):
    start_response('200 OK', [('Content-type', 'text/plain')])
    yield b'First generated line\n'
    yield b'Second generated line\n'
