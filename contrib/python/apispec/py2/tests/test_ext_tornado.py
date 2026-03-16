# -*- coding: utf-8 -*-
import pytest

from apispec import APISpec
from apispec.ext.tornado import TornadoPlugin
from tornado.web import RequestHandler
import tornado.gen

@pytest.fixture(params=(True, False))
def spec(request):
    return APISpec(
        title='Swagger Petstore',
        version='1.0.0',
        description='This is a sample Petstore server.  You can find out more '
        'about Swagger at <a href=\"http://swagger.wordnik.com\">'
        'http://swagger.wordnik.com</a> or on irc.freenode.net, #swagger.'
        'For this sample, you can use the api key \"special-key\" to test the'
        'authorization filters',
        plugins=[
            # Test both plugin class and deprecated interface
            TornadoPlugin() if request.param else 'apispec.ext.tornado',
        ],
    )


class TestPathHelpers:

    def test_path_from_urlspec(self, spec):
        class HelloHandler(RequestHandler):
            def get(self):
                self.write('hello')

        urlspec = (r'/hello', HelloHandler)
        operations = (
            {'get': {'parameters': [], 'responses': {'200': {}}}}
        )

        spec.add_path(urlspec=urlspec, operations=operations)
        assert '/hello' in spec._paths
        assert 'get' in spec._paths['/hello']
        expected = {'parameters': [], 'responses': {'200': {}}}
        assert spec._paths['/hello']['get'] == expected

    def test_path_with_multiple_methods(self, spec):

        class HelloHandler(RequestHandler):
            def get(self):
                self.write('hello')

            def post(self):
                self.write('hello')

        urlspec = (r'/hello', HelloHandler)
        operations = {
            'get': {
                'description': 'get a greeting',
                'responses': {'200': {}},
            },
            'post': {
                'description': 'post a greeting',
                'responses': {'200': {}},
            },
        }
        spec.add_path(urlspec=urlspec, operations=operations)
        get_op = spec._paths['/hello']['get']
        post_op = spec._paths['/hello']['post']
        assert get_op['description'] == 'get a greeting'
        assert post_op['description'] == 'post a greeting'

    def test_integration_with_docstring_introspection(self, spec):

        class HelloHandler(RequestHandler):
            """
            ---
            x-extension: value
            """
            def get(self):
                """Get a greeting endpoint.
                ---
                description: get a greeting
                responses:
                    200:
                        description: a pet to be returned
                        schema:
                            $ref: #/definitions/Pet
                """
                self.write('hello')

            def post(self):
                """Post a greeting endpoint.
                ---
                description: post a greeting
                responses:
                    200:
                        description: some data
                """
                self.write('hello')

        urlspec = (r'/hello', HelloHandler)
        spec.add_path(urlspec=urlspec)
        get_op = spec._paths['/hello']['get']
        post_op = spec._paths['/hello']['post']
        extension = spec._paths['/hello']['x-extension']
        assert get_op['description'] == 'get a greeting'
        assert post_op['description'] == 'post a greeting'
        assert extension == 'value'

    def test_path_removing_trailing_or_optional_slash(self, spec):

        class HelloHandler(RequestHandler):
            def get(self):
                self.write('hello world')

        urlspec = (r'/hello/world/*', HelloHandler)
        operations = (
            {'get': {'parameters': [], 'responses': {'200': {}}}}
        )

        spec.add_path(urlspec=urlspec, operations=operations)
        assert '/hello/world' in spec._paths

    class HelloWorldHandler(RequestHandler):
        def get(self, param1, param2):
            self.write('hello')

    class HelloWorldHandler2(RequestHandler):
        @tornado.gen.coroutine
        def get(self, param1, param2):
            self.write('hello')

    @pytest.mark.parametrize(
        'Handler',
        [
            HelloWorldHandler,
            HelloWorldHandler2,
        ],
    )
    def test_path_with_params(self, spec, Handler):
        urlspec = (r'/hello/([^/]+)/world/([^/]+)', Handler)
        operations = (
            {'get': {'parameters': [], 'responses': {'200': {}}}}
        )

        spec.add_path(urlspec=urlspec, operations=operations)
        path = '/hello/{param1}/world/{param2}'
        assert path in spec._paths
        assert 'get' in spec._paths[path]
        expected = {'parameters': [], 'responses': {'200': {}}}
        assert spec._paths[path]['get'] == expected
