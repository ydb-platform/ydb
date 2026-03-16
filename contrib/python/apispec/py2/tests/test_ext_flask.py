# -*- coding: utf-8 -*-
import pytest

from flask import Flask
from flask.views import MethodView
from apispec import APISpec
from apispec.ext.flask import FlaskPlugin


@pytest.fixture(params=(True, False))
def spec(request):
    return APISpec(
        title='Swagger Petstore',
        version='1.0.0',
        description='This is a sample Petstore server.  You can find out more '
        'about Swagger at <a href=\"http://swagger.wordnik.com\">http://swagger.wordnik.com</a> '
        'or on irc.freenode.net, #swagger.  For this sample, you can use the api '
        'key \"special-key\" to test the authorization filters',
        plugins=[
            # Test both plugin class and deprecated interface
            FlaskPlugin() if request.param else 'apispec.ext.flask',
        ],
    )


@pytest.yield_fixture()
def app():
    _app = Flask(__name__)
    ctx = _app.test_request_context()
    ctx.push()
    yield _app
    ctx.pop()


class TestPathHelpers:

    def test_path_from_view(self, app, spec):
        @app.route('/hello')
        def hello():
            return 'hi'

        spec.add_path(
            view=hello,
            operations={'get': {'parameters': [], 'responses': {'200': {}}}},
        )
        assert '/hello' in spec._paths
        assert 'get' in spec._paths['/hello']
        expected = {'parameters': [], 'responses': {'200': {}}}
        assert spec._paths['/hello']['get'] == expected

    def test_path_from_method_view(self, app, spec):
        class HelloApi(MethodView):
            """Greeting API.
            ---
            x-extension: global metadata
            """
            def get(self):
                """A greeting endpoint.
                ---
                description: get a greeting
                responses:
                    200:
                        description: said hi
                """
                return 'hi'

            def post(self):
                return 'hi'

        method_view = HelloApi.as_view('hi')
        app.add_url_rule('/hi', view_func=method_view, methods=('GET', 'POST'))
        spec.add_path(view=method_view)
        expected = {
            'description': 'get a greeting',
            'responses': {200: {'description': 'said hi'}},
        }
        assert spec._paths['/hi']['get'] == expected
        assert spec._paths['/hi']['post'] == {}
        assert spec._paths['/hi']['x-extension'] == 'global metadata'

    def test_path_with_multiple_methods(self, app, spec):

        @app.route('/hello', methods=['GET', 'POST'])
        def hello():
            return 'hi'

        spec.add_path(
            view=hello, operations=dict(
                get={'description': 'get a greeting', 'responses': {'200': {}}},
                post={'description': 'post a greeting', 'responses': {'200': {}}},
            ),
        )
        get_op = spec._paths['/hello']['get']
        post_op = spec._paths['/hello']['post']
        assert get_op['description'] == 'get a greeting'
        assert post_op['description'] == 'post a greeting'

    def test_methods_from_rule(self, app, spec):
        class HelloApi(MethodView):
            """Greeting API.
            ---
            x-extension: global metadata
            """
            def get(self):
                """A greeting endpoint.
                ---
                description: get a greeting
                responses:
                    200:
                        description: said hi
                """
                return 'hi'

            def post(self):
                return 'hi'

            def delete(self):
                return 'hi'

        method_view = HelloApi.as_view('hi')
        app.add_url_rule('/hi', view_func=method_view, methods=('GET', 'POST'))
        spec.add_path(view=method_view)
        assert 'get' in spec._paths['/hi']
        assert 'post' in spec._paths['/hi']
        assert 'delete' not in spec._paths['/hi']

    def test_integration_with_docstring_introspection(self, app, spec):

        @app.route('/hello')
        def hello():
            """A greeting endpoint.

            ---
            x-extension: value
            get:
                description: get a greeting
                responses:
                    200:
                        description: a pet to be returned
                        schema:
                            $ref: #/definitions/Pet

            post:
                description: post a greeting
                responses:
                    200:
                        description: some data

            foo:
                description: not a valid operation
                responses:
                    200:
                        description:
                            more junk
            """
            return 'hi'

        spec.add_path(view=hello)
        get_op = spec._paths['/hello']['get']
        post_op = spec._paths['/hello']['post']
        extension = spec._paths['/hello']['x-extension']
        assert get_op['description'] == 'get a greeting'
        assert post_op['description'] == 'post a greeting'
        assert 'foo' not in spec._paths['/hello']
        assert extension == 'value'

    def test_path_is_translated_to_swagger_template(self, app, spec):

        @app.route('/pet/<pet_id>')
        def get_pet(pet_id):
            return 'representation of pet {pet_id}'.format(pet_id=pet_id)

        spec.add_path(view=get_pet)
        assert '/pet/{pet_id}' in spec._paths

    def test_path_includes_app_root(self, app, spec):

        spec.options['basePath'] = '/v1'
        app.config['APPLICATION_ROOT'] = '/v1/app/root'

        @app.route('/partial/path/pet')
        def get_pet():
            return 'pet'

        spec.add_path(view=get_pet)
        assert '/app/root/partial/path/pet' in spec._paths

    def test_path_with_args_includes_app_root(self, app, spec):

        spec.options['basePath'] = '/v1'
        app.config['APPLICATION_ROOT'] = '/v1/app/root'

        @app.route('/partial/path/pet/{pet_id}')
        def get_pet(pet_id):
            return 'representation of pet {pet_id}'.format(pet_id=pet_id)

        spec.add_path(view=get_pet)
        assert '/app/root/partial/path/pet/{pet_id}' in spec._paths

    def test_path_includes_app_root_with_right_slash(self, app, spec):

        spec.options['basePath'] = '/v1'
        app.config['APPLICATION_ROOT'] = '/v1/app/root/'

        @app.route('/partial/path/pet')
        def get_pet():
            return 'pet'

        spec.add_path(view=get_pet)
        assert '/app/root/partial/path/pet' in spec._paths
