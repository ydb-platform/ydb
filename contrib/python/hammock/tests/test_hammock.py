import unittest

from httpretty import HTTPretty
from httpretty import httprettified

from hammock import Hammock


class TestCaseWrest(unittest.TestCase):

    HOST = 'localhost'
    PORT = 8000
    BASE_URL = 'http://%s:%s' % (HOST, PORT)
    PATH = '/sample/path/to/resource'
    URL = BASE_URL + PATH

    @httprettified
    def test_methods(self):
        client = Hammock(self.BASE_URL)
        for method in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
            HTTPretty.register_uri(getattr(HTTPretty, method), self.URL)
            request = getattr(client, method)
            request('sample', 'path', 'to', 'resource')
            self.assertEqual(HTTPretty.last_request.method, method)

    @httprettified
    def test_urls(self):
        HTTPretty.register_uri(HTTPretty.GET, self.URL)
        client = Hammock(self.BASE_URL)
        combs = [
            client.sample.path.to.resource,
            client('sample').path('to').resource,
            client('sample', 'path', 'to', 'resource'),
            client('sample')('path')('to')('resource'),
            client.sample('path')('to', 'resource'),
            client('sample', 'path',).to.resource
        ]

        for comb in combs:
            self.assertEqual(str(comb), self.URL)
            comb.GET()
            self.assertEqual(HTTPretty.last_request.path, self.PATH)

    @httprettified
    def test_append_slash_option(self):
        HTTPretty.register_uri(HTTPretty.GET, self.URL + '/')
        client = Hammock(self.BASE_URL, append_slash=True)
        client.sample.path.to.resource.GET()
        self.assertEqual(HTTPretty.last_request.path, self.PATH + '/')

    @httprettified
    def test_inheritance(self):
        """https://github.com/kadirpekel/hammock/pull/5/files#L1R99"""
        class CustomHammock(Hammock):
            def __init__(self, name=None, parent=None, **kwargs):
                if 'testing' in kwargs:
                    self.testing = kwargs.pop('testing')
                super(CustomHammock, self).__init__(name, parent, **kwargs)

            def _url(self, *args):
                assert isinstance(self.testing, bool)
                global called
                called = True
                return super(CustomHammock, self)._url(*args)

        global called
        called = False
        HTTPretty.register_uri(HTTPretty.GET, self.URL)
        client = CustomHammock(self.BASE_URL, testing=True)
        client.sample.path.to.resource.GET()
        self.assertTrue(called)
        self.assertEqual(HTTPretty.last_request.path, self.PATH)

    @httprettified
    def test_session(self):
        ACCEPT_HEADER = 'application/json'
        kwargs = {
            'headers': {'Accept': ACCEPT_HEADER},
            'auth': ('foo', 'bar'),
        }
        client = Hammock(self.BASE_URL, **kwargs)
        HTTPretty.register_uri(HTTPretty.GET, self.URL)
        client.sample.path.to.resource.GET()
        request = HTTPretty.last_request
        self.assertIn('Authorization', request.headers)
        self.assertIn('Accept', request.headers)
        self.assertEqual(request.headers.get('Accept'), ACCEPT_HEADER)
        client.sample.path.to.resource.GET()
        request = HTTPretty.last_request
        self.assertIn('Authorization', request.headers)
        self.assertIn('Accept', request.headers)
        self.assertEqual(request.headers.get('Accept'), ACCEPT_HEADER)
