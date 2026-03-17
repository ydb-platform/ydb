# -*- coding: utf-8 -*-
import pytest

from bottle import route
from apispec import APISpec
from apispec.ext.bottle import BottlePlugin

@pytest.fixture(params=(True, False))
def spec(request):
    return APISpec(
        title='Swagger Petstore',
        version='1.0.0',
        description='This is a sample Petstore server.  You can find out more '
                    'about Swagger at <a href=\"http://swagger.wordnik.com\">http://swagger.wordnik.com</a> '
                    'or on irc.freenode.net, #swagger.  For this sample, you can use the api '
                    'key \"special-key\" to test the authorization filters',
        plugins=(
            # Test both plugin class and deprecated interface
            BottlePlugin() if request.param else 'apispec.ext.bottle',
        ),
    )


class TestPathHelpers:

    def test_path_from_view(self, spec):
        @route('/hello')
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

    def test_path_with_multiple_methods(self, spec):

        @route('/hello', methods=['GET', 'POST'])
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

    def test_integration_with_docstring_introspection(self, spec):

        @route('/hello')
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

    def test_path_is_translated_to_openapi_template(self, spec):

        @route('/pet/<pet_id>')
        def get_pet(pet_id):
            return 'representation of pet {pet_id}'.format(pet_id=pet_id)

        spec.add_path(view=get_pet)
        assert '/pet/{pet_id}' in spec._paths

    def test_path_with_params(self, spec):

        @route('/pet/<pet_id>/<shop_id>', methods=['POST'])
        def set_pet():
            return 'new pet!'

        spec.add_path(view=set_pet)
        assert '/pet/{pet_id}/{shop_id}' in spec._paths
