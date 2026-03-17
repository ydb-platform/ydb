# -*- coding: utf-8 -*-
from collections import OrderedDict

import pytest
import mock
import yaml

from apispec import APISpec, BasePlugin, Path
from apispec.exceptions import PluginError, APISpecError


description = 'This is a sample Petstore server.  You can find out more '
'about Swagger at <a href=\"http://swagger.wordnik.com\">http://swagger.wordnik.com</a> '
'or on irc.freenode.net, #swagger.  For this sample, you can use the api '
'key \"special-key\" to test the authorization filters'


@pytest.fixture(params=('2.0', '3.0.0'))
def spec(request):
    openapi_version = request.param
    if openapi_version == '2.0':
        security_kwargs = {
            'security': [{'apiKey': []}],
        }
    else:
        security_kwargs = {
            'components': {
                'securitySchemes': {
                    'bearerAuth':
                        dict(type='http', scheme='bearer', bearerFormat='JWT'),
                },
                'schemas': {
                    'ErrorResponse': {
                        'type': 'object',
                        'properties': {
                            'ok': {
                                'type': 'boolean', 'description': 'status indicator', 'example': False,
                            },
                        },
                        'required': ['ok'],
                    },
                },
            },
        }
    return APISpec(
        title='Swagger Petstore',
        version='1.0.0',
        info={'description': description},
        openapi_version=openapi_version,
        **security_kwargs
    )


class TestAPISpecInit:

    def test_raises_wrong_apispec_version(self):
        with pytest.raises(APISpecError) as excinfo:
            APISpec(
                'Swagger Petstore',
                version='1.0.0',
                info={'description': description},
                security=[{'apiKey': []}],
                openapi_version='4.0',  # 4.0 is not supported
            )
        assert 'Not a valid OpenAPI version number:' in str(excinfo)


class TestMetadata:

    def test_openapi_metadata(self, spec):
        metadata = spec.to_dict()
        assert metadata['info']['title'] == 'Swagger Petstore'
        assert metadata['info']['version'] == '1.0.0'
        assert metadata['info']['description'] == description
        if spec.openapi_version.major < 3:
            assert metadata['swagger'] == spec.openapi_version.vstring
            assert metadata['security'] == [{'apiKey': []}]
        else:
            assert metadata['openapi'] == spec.openapi_version.vstring
            security_schemes = {'bearerAuth': dict(type='http', scheme='bearer', bearerFormat='JWT')}
            assert metadata['components']['securitySchemes'] == security_schemes
            assert metadata['components']['schemas'].get('ErrorResponse', False)
            assert metadata['info']['title'] == 'Swagger Petstore'
            assert metadata['info']['version'] == '1.0.0'
            assert metadata['info']['description'] == description

    @pytest.mark.parametrize('spec', ('3.0.0', ), indirect=True)
    def test_openapi_metadata_merge_v3(self, spec):
        properties = {
            'ok': {
                'type': 'boolean', 'description': 'property description', 'example': True,
            },
        }
        spec.definition('definition', properties=properties, description='definiton description')
        metadata = spec.to_dict()
        assert metadata['components']['schemas'].get('ErrorResponse', False)
        assert metadata['components']['schemas'].get('definition', False)


class TestTags:

    tag = {
        'name': 'MyTag',
        'description': 'This tag gathers all API endpoints which are mine.',
    }

    def test_tag(self, spec):
        spec.add_tag(self.tag)
        tags_json = spec.to_dict()['tags']
        assert self.tag in tags_json


class TestDefinitions:

    properties = {
        'id': {'type': 'integer', 'format': 'int64'},
        'name': {'type': 'string', 'example': 'doggie'},
    }

    def test_definition(self, spec):
        spec.definition('Pet', properties=self.properties)
        if spec.openapi_version.major < 3:
            defs = spec.to_dict()['definitions']
        else:
            defs = spec.to_dict()['components']['schemas']
        assert 'Pet' in defs
        assert defs['Pet']['properties'] == self.properties

    def test_definition_description(self, spec):
        model_description = 'An animal which lives with humans.'
        spec.definition('Pet', properties=self.properties, description=model_description)
        if spec.openapi_version.major < 3:
            defs = spec.to_dict()['definitions']
        else:
            defs = spec.to_dict()['components']['schemas']
        assert defs['Pet']['description'] == model_description

    def test_definition_stores_enum(self, spec):
        enum = ['name', 'photoUrls']
        spec.definition(
            'Pet',
            properties=self.properties,
            enum=enum,
        )
        if spec.openapi_version.major < 3:
            defs = spec.to_dict()['definitions']
        else:
            defs = spec.to_dict()['components']['schemas']
        assert defs['Pet']['enum'] == enum

    def test_definition_extra_fields(self, spec):
        extra_fields = {'discriminator': 'name'}
        spec.definition('Pet', properties=self.properties, extra_fields=extra_fields)
        if spec.openapi_version.major < 3:
            defs = spec.to_dict()['definitions']
        else:
            defs = spec.to_dict()['components']['schemas']
        assert defs['Pet']['discriminator'] == 'name'

    def test_pass_definition_to_plugins(self, spec):
        def def_helper(spec, name, **kwargs):
            if kwargs.get('definition') is not None:
                return {'available': True}

            return {'available': False}

        spec.register_definition_helper(def_helper)
        spec.definition('Pet', properties=self.properties)

        if spec.openapi_version.major < 3:
            defs = spec.to_dict()['definitions']
        else:
            defs = spec.to_dict()['components']['schemas']

        assert defs['Pet']['available']

    def test_to_yaml(self, spec):
        enum = ['name', 'photoUrls']
        spec.definition(
            'Pet',
            properties=self.properties,
            enum=enum,
        )
        assert spec.to_dict() == yaml.load(spec.to_yaml())

class TestPath:
    paths = {
        '/pet/{petId}': {
            'get': {
                'parameters': [
                    {
                        'required': True,
                        'format': 'int64',
                        'name': 'petId',
                        'in': 'path',
                        'type': 'integer',
                        'description': 'ID of pet that needs to be fetched',
                    },
                ],
                'responses': {
                    '200': {
                        'schema': {'$ref': '#/definitions/Pet'},
                        'description': 'successful operation',
                    },
                    '400': {
                        'description': 'Invalid ID supplied',
                    },
                    '404': {
                        'description': 'Pet not found',
                    },
                },
                'produces': [
                    'application/json',
                    'application/xml',
                ],
                'operationId': 'getPetById',
                'summary': 'Find pet by ID',
                'description': (
                    'Returns a pet when ID < 10.  '
                    'ID > 10 or nonintegers will simulate API error conditions'
                ),
                'tags': ['pet'],
            },
        },
    }

    def test_add_path(self, spec):
        route_spec = self.paths['/pet/{petId}']['get']
        spec.add_path(
            path='/pet/{petId}',
            operations=dict(
                get=dict(
                    parameters=route_spec['parameters'],
                    responses=route_spec['responses'],
                    produces=route_spec['produces'],
                    operationId=route_spec['operationId'],
                    summary=route_spec['summary'],
                    description=route_spec['description'],
                    tags=route_spec['tags'],
                ),
            ),
        )

        p = spec._paths['/pet/{petId}']['get']
        assert p['parameters'] == route_spec['parameters']
        assert p['responses'] == route_spec['responses']
        assert p['operationId'] == route_spec['operationId']
        assert p['summary'] == route_spec['summary']
        assert p['description'] == route_spec['description']
        assert p['tags'] == route_spec['tags']

    def test_paths_maintain_order(self, spec):
        spec.add_path(path='/path1')
        spec.add_path(path='/path2')
        spec.add_path(path='/path3')
        spec.add_path(path='/path4')
        assert list(spec.to_dict()['paths'].keys()) == ['/path1', '/path2', '/path3', '/path4']

    def test_methods_maintain_order(self, spec):
        methods = ['get', 'post', 'put', 'patch', 'delete', 'head', 'options']
        for method in methods:
            spec.add_path(path='/path', operations=OrderedDict({method: {}}))
        assert list(spec.to_dict()['paths']['/path']) == methods

    def test_add_path_merges_paths(self, spec):
        """Test that adding a second HTTP method to an existing path performs
        a merge operation instead of an overwrite"""
        path = '/pet/{petId}'
        route_spec = self.paths[path]['get']
        spec.add_path(
            path=path,
            operations=dict(
                get=route_spec,
            ),
        )
        spec.add_path(
            path=path,
            operations=dict(
                put=dict(
                    parameters=route_spec['parameters'],
                    responses=route_spec['responses'],
                    produces=route_spec['produces'],
                    operationId='updatePet',
                    summary='Updates an existing Pet',
                    description='Use this method to make changes to Pet `petId`',
                    tags=route_spec['tags'],
                ),
            ),
        )

        p = spec._paths[path]
        assert 'get' in p
        assert 'put' in p

    def test_add_path_ensures_path_parameters_required(self, spec):
        path = '/pet/{petId}'
        spec.add_path(
            path=path,
            operations=dict(
                put=dict(
                    parameters=[{
                        'name': 'petId',
                        'in': 'path',
                    }],
                ),
            ),
        )
        assert spec._paths[path]['put']['parameters'][0]['required'] is True

    def test_add_path_with_no_path_raises_error(self, spec):
        with pytest.raises(APISpecError) as excinfo:
            spec.add_path()
        assert 'Path template is not specified' in str(excinfo)

    def test_add_path_strips_base_path(self, spec):
        spec.options['basePath'] = '/v1'
        spec.add_path('/v1/pets')
        assert '/pets' in spec._paths
        assert '/v1/pets' not in spec._paths

    def test_add_path_accepts_path(self, spec):
        route = '/pet/{petId}'
        route_spec = self.paths[route]
        path = Path(path=route, operations={'get': route_spec['get']})
        spec.add_path(path)

        p = spec._paths[path.path]
        assert p == path.operations
        assert 'get' in p

    def test_add_path_strips_path_base_path(self, spec):
        spec.options['basePath'] = '/v1'
        path = Path(path='/v1/pets')
        spec.add_path(path)
        assert '/pets' in spec._paths
        assert '/v1/pets' not in spec._paths

    def test_add_parameters(self, spec):
        route_spec = self.paths['/pet/{petId}']['get']

        spec.add_parameter('test_parameter', 'path', **route_spec['parameters'][0])

        spec.add_path(
            path='/pet/{petId}',
            operations=dict(
                get=dict(
                    parameters=['test_parameter'],
                ),
            ),
        )

        metadata = spec.to_dict()
        p = spec._paths['/pet/{petId}']['get']

        if spec.openapi_version.major < 3:
            assert p['parameters'][0] == {'$ref': '#/parameters/test_parameter'}
            assert route_spec['parameters'][0] == metadata['parameters']['test_parameter']
        else:
            assert p['parameters'][0] == {'$ref': '#/components/parameters/test_parameter'}
            assert route_spec['parameters'][0] == metadata['components']['parameters']['test_parameter']


class TestPlugins:

    class TestPlugin(BasePlugin):
        def definition_helper(self, name, definition, **kwargs):
            return {'properties': {'name': {'type': 'string'}}}

        def path_helper(self, path, **kwargs):
            if path.path == '/path_1':
                return Path(
                    path='/path_1_modified',
                    operations={'get': {'responses': {'200': {}}}},
                )

        def operation_helper(self, path, operations, **kwargs):
            if path.path == '/path_2':
                operations['post'] = {'responses': {'201': {}}}

        def response_helper(self, method, status_code, **kwargs):
            if method == 'delete':
                return {'description': 'Clever description'}

    def test_plugin_definition_helper_is_used(self):
        spec = APISpec(
            title='Swagger Petstore',
            version='1.0.0',
            plugins=(self.TestPlugin(), ),
        )
        spec.definition('Pet', {})
        assert spec._definitions['Pet'] == {'properties': {'name': {'type': 'string'}}}

    def test_plugin_path_helper_is_used(self):
        spec = APISpec(
            title='Swagger Petstore',
            version='1.0.0',
            plugins=(self.TestPlugin(), ),
        )
        spec.add_path('/path_1')
        assert len(spec._paths) == 1
        assert spec._paths['/path_1_modified'] == {'get': {'responses': {'200': {}}}}

    def test_plugin_operation_helper_is_used(self):
        spec = APISpec(
            title='Swagger Petstore',
            version='1.0.0',
            plugins=(self.TestPlugin(), ),
        )
        spec.add_path('/path_2', operations={'post': {'responses': {'200': {}}}})
        assert len(spec._paths) == 1
        assert spec._paths['/path_2'] == {'post': {'responses': {'201': {}}}}

    def test_plugin_response_helper_is_used(self, spec):
        spec = APISpec(
            title='Swagger Petstore',
            version='1.0.0',
            plugins=(self.TestPlugin(), ),
        )
        spec.add_path('/path_3', operations={'delete': {'responses': {'204': {'content': {}}}}})
        assert len(spec._paths) == 1
        assert spec._paths['/path_3'] == {'delete': {'responses': {'204': {
            'content': {}, 'description': 'Clever description',
        }}}}


class TestOldPlugins:

    DUMMY_PLUGIN = '__tests__.plugins.dummy_plugin'

    @mock.patch(DUMMY_PLUGIN + '.setup', autospec=True)
    def test_setup_plugin(self, mock_setup, spec):
        spec.setup_plugin(self.DUMMY_PLUGIN)
        assert self.DUMMY_PLUGIN in spec.old_plugins
        mock_setup.assert_called_once_with(spec)
        spec.setup_plugin(self.DUMMY_PLUGIN)
        assert mock_setup.call_count == 1

    def test_setup_can_modify_plugin_dict(self, spec):
        spec.setup_plugin(self.DUMMY_PLUGIN)
        spec.old_plugins[self.DUMMY_PLUGIN]['foo'] == 42

    def test_setup_plugin_doesnt_exist(self, spec):
        with pytest.raises(PluginError):
            spec.setup_plugin('plugin.doesnt.exist')

    def test_setup_plugin_with_no_setup_function_raises_error(self, spec):
        plugin_path = '__tests__.plugins.dummy_plugin_no_setup'
        with pytest.raises(PluginError) as excinfo:
            spec.setup_plugin(plugin_path)
        msg = excinfo.value.args[0]
        assert msg == 'Plugin "{0}" has no setup(spec) function'.format(plugin_path)

    def test_register_definition_helper(self, spec):
        def my_definition_helper(name, schema, **kwargs):
            pass
        spec.register_definition_helper(my_definition_helper)
        assert my_definition_helper in spec._definition_helpers

    def test_register_path_helper(self, spec):
        def my_path_helper(**kwargs):
            pass

        spec.register_path_helper(my_path_helper)
        assert my_path_helper in spec._path_helpers

    def test_multiple_path_helpers_w_different_signatures(self, spec):
        def helper1(spec, spam, **kwargs):
            return Path(path='/foo/bar')

        def helper2(spec, eggs, **kwargs):
            return Path(path='/foo/bar')

        spec.register_path_helper(helper1)
        spec.register_path_helper(helper2)

        spec.add_path(eggs=object())

    def test_multiple_definition_helpers_w_different_signatures(self, spec):
        def helper1(spec, name, spam, **kwargs):
            return mock.MagicMock()

        def helper2(spec, name, eggs, **kwargs):
            return mock.MagicMock()

        spec.register_definition_helper(helper1)
        spec.register_definition_helper(helper2)

        spec.definition('SpammitySpam', eggs=mock.MagicMock())


class TestDefinitionHelpers:

    def test_definition_helpers_are_used(self, spec):
        properties = {'properties': {'name': {'type': 'string'}}}

        def definition_helper(spec, name, **kwargs):
            assert type(spec) == APISpec
            return properties
        spec.register_definition_helper(definition_helper)
        spec.definition('Pet', {})
        assert spec._definitions['Pet'] == properties

    def test_multiple_definition_helpers(self, spec):
        def helper1(spec, name, **kwargs):
            return {'properties': {'age': {'type': 'number'}}}

        def helper2(spec, name, fmt, **kwargs):
            return {'properties': {'age': {'type': 'number', 'format': fmt}}}

        spec.register_definition_helper(helper1)
        spec.register_definition_helper(helper2)
        spec.definition('Pet', fmt='int32')
        expected = {'properties': {'age': {'type': 'number', 'format': 'int32'}}}
        assert spec._definitions['Pet'] == expected


class TestPathHelpers:

    def test_path_helper_is_used(self, spec):
        def path_helper(spec, view_func, **kwargs):
            return Path(path=view_func['path'])
        spec.register_path_helper(path_helper)
        spec.add_path(
            view_func={'path': '/pet/{petId}'},
            operations=dict(
                get=dict(
                    produces=('application/xml', ),
                    responses={
                        '200': {
                            'schema': {'$ref': '#/definitions/Pet'},
                            'description': 'successful operation',
                        },
                    },
                ),
            ),
        )
        expected = {
            '/pet/{petId}': {
                'get': {
                    'produces': ('application/xml', ),
                    'responses': {
                        '200': {
                            'schema': {'$ref': '#/definitions/Pet'},
                            'description': 'successful operation',
                        },
                    },
                },
            },
        }

        assert spec._paths == expected

class TestResponseHelpers:

    def test_response_helper_is_used(self, spec):
        def success_helper(spec, success_description, **kwargs):
            return {'description': success_description}

        spec.register_response_helper(success_helper, 'get', 200)
        spec.add_path(
            '/pet/{petId}', success_description='success!', operations={
                'get': {
                    'responses': {
                        200: {
                            'schema': {'$ref': 'Pet'},
                        },
                    },
                },
            },
        )

        resp_obj = spec._paths['/pet/{petId}']['get']['responses'][200]
        assert resp_obj['schema'] == {'$ref': 'Pet'}
        assert resp_obj['description'] == 'success!'


def test_helper_functions_deprecation_warning(spec, recwarn):
    with pytest.deprecated_call():
        spec.register_definition_helper(lambda x: x)
    with pytest.deprecated_call():
        spec.register_path_helper(lambda x: x)
    with pytest.deprecated_call():
        spec.register_operation_helper(lambda x: x)
    with pytest.deprecated_call():
        spec.register_response_helper(lambda x: x, 'GET', 200)

def test_schema_name_resolver_deprecation_warning(recwarn):
    with pytest.deprecated_call():
        APISpec(
            title='Swagger Petstore', version='1.0.0',
            schema_name_resolver=lambda x: x,
        )

def test_plugins_as_string_deprecation_warning(recwarn):
    with pytest.deprecated_call():
        APISpec(
            title='Swagger Petstore', version='1.0.0',
            plugins=('__tests__.plugins.dummy_plugin', ),
        )

def test_setup_plugin_deprecation_warning(spec, recwarn):
    with pytest.deprecated_call():
        spec.setup_plugin('__tests__.plugins.dummy_plugin', )
