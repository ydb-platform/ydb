# -*- coding: utf-8 -*-
"""Marshmallow plugin for apispec. Allows passing a marshmallow
`Schema` to `APISpec.definition <apispec.APISpec.definition>`
and `APISpec.add_path <apispec.APISpec.add_path>` (for responses).

Requires marshmallow>=2.0.

::

    from pprint import pprint

    from marshmallow import Schema, fields

    class UserSchema(Schema):
        id = fields.Int(dump_only=True)
        name = fields.Str(description="The user's name")

    spec.definition('User', schema=UserSchema)
    pprint(spec.to_dict()['definitions'])
    # {'User': {'properties': {'id': {'format': 'int32', 'type': 'integer'},
    #                         'name': {'description': "The user's name",
    #                                 'type': 'string'}},
    #         'type': 'object'}}

"""
from __future__ import absolute_import

import marshmallow

from apispec import Path, BasePlugin, utils
from .common import resolve_schema_cls, resolve_schema_instance
from .openapi import OpenAPIConverter


class MarshmallowPlugin(BasePlugin):
    """APISpec plugin handling marshmallow schemas

    :param callable schema_name_resolver: Callable to generate the schema definition name.
        Receives the `Schema` class and returns the name to be used in refs within
        the generated spec.

        Example: ::

            def schema_name_resolver(schema):
                return schema.__name__
    """
    def __init__(self, schema_name_resolver=None):
        super(MarshmallowPlugin, self).__init__()
        self.schema_name_resolver = schema_name_resolver
        self.spec = None
        self.openapi_version = None
        self.openapi = None

    def init_spec(self, spec):
        super(MarshmallowPlugin, self).init_spec(spec)
        self.spec = spec
        self.openapi_version = spec.openapi_version
        self.openapi = OpenAPIConverter(openapi_version=spec.openapi_version)

    def inspect_schema_for_auto_referencing(self, original_schema_instance):
        """Parse given schema instance and reference eventual nested schemas
        :param original_schema_instance: schema to parse
        """
        # schema_name_resolver must be provided to use this function
        assert self.schema_name_resolver

        for field in original_schema_instance.fields.values():
            nested_schema_class = None

            if isinstance(field, marshmallow.fields.Nested):
                nested_schema_class = resolve_schema_cls(field.schema)

            elif isinstance(field, marshmallow.fields.List) \
                    and isinstance(field.container, marshmallow.fields.Nested):
                nested_schema_class = resolve_schema_cls(field.container.schema)

            if nested_schema_class and nested_schema_class not in self.openapi.refs:
                definition_name = self.schema_name_resolver(
                    nested_schema_class,
                )
                if definition_name:
                    self.spec.definition(
                        definition_name,
                        schema=nested_schema_class,
                    )

    def resolve_parameters(self, parameters):
        resolved = []
        for parameter in parameters:
            if not isinstance(parameter.get('schema', {}), dict):
                schema_cls = resolve_schema_cls(parameter['schema'])
                if issubclass(schema_cls, marshmallow.Schema) and 'in' in parameter:
                    del parameter['schema']
                    resolved += self.openapi.schema2parameters(
                        schema_cls,
                        default_in=parameter.pop('in'), **parameter
                    )
                    continue
            self.resolve_schema(parameter)
            resolved.append(parameter)
        return resolved

    def resolve_schema_in_request_body(self, request_body):
        """Function to resolve a schema in a requestBody object - modifies then
        response dict to convert Marshmallow Schema object or class into dict
        """
        content = request_body['content']
        for content_type in content:
            schema = content[content_type]['schema']
            content[content_type]['schema'] = self.openapi.resolve_schema_dict(schema)

    def resolve_schema(self, data):
        """Function to resolve a schema in a parameter or response - modifies the
        corresponding dict to convert Marshmallow Schema object or class into dict

        :param APISpec spec: `APISpec` containing refs.
        :param dict data: the parameter or response dictionary that may contain a schema
        """
        if self.openapi_version.major < 3:
            if 'schema' in data:
                data['schema'] = self.openapi.resolve_schema_dict(data['schema'])
        else:
            if 'content' in data:
                for content_type in data['content']:
                    schema = data['content'][content_type]['schema']
                    data['content'][content_type]['schema'] = self.openapi.resolve_schema_dict(schema)

    def map_to_openapi_type(self, *args):
        """Decorator to set mapping for custom fields.

        ``*args`` can be:

        - a pair of the form ``(type, format)``
        - a core marshmallow field type (in which case we reuse that type's mapping)

        Examples: ::

            @ma_plugin.map_to_openapi_type('string', 'uuid')
            class MyCustomField(Integer):
                # ...

            @ma_plugin.map_to_openapi_type(Integer)  # will map to ('integer', 'int32')
            class MyCustomFieldThatsKindaLikeAnInteger(Integer):
                # ...
        """
        return self.openapi.map_to_openapi_type(*args)

    def definition_helper(self, name, schema, **kwargs):
        """Definition helper that allows using a marshmallow
        :class:`Schema <marshmallow.Schema>` to provide OpenAPI
        metadata.

        :param type|Schema schema: A marshmallow Schema class or instance.
        """

        schema_cls = resolve_schema_cls(schema)
        schema_instance = resolve_schema_instance(schema)

        # Store registered refs, keyed by Schema class
        self.openapi.refs[schema_cls] = name

        json_schema = self.openapi.schema2jsonschema(schema_instance, name=name)

        # Auto reference schema if schema_name_resolver
        if self.schema_name_resolver:
            self.inspect_schema_for_auto_referencing(schema_instance)

        return json_schema

    def path_helper(self, view=None, **kwargs):
        """Path helper that allows passing a Schema as a response. Responses can be
        defined in a view's docstring.
        ::

            from pprint import pprint

            from my_app import Users, UserSchema

            class UserHandler:
                def get(self, user_id):
                    '''Get a user endpoint.
                    ---
                    description: Get a user
                    responses:
                        200:
                            description: A user
                            schema: UserSchema
                    '''
                    user = Users.get(id=user_id)
                    schema = UserSchema()
                    return schema.dumps(user)

            urlspec = (r'/users/{user_id}', UserHandler)
            spec.add_path(urlspec=urlspec)
            pprint(spec.to_dict()['paths'])
            # {'/users/{user_id}': {'get': {'description': 'Get a user',
            #                               'responses': {200: {'description': 'A user',
            #                                                   'schema': {'$ref': '#/definitions/User'}}}}}}

        ::

            from pprint import pprint

            from my_app import Users, UserSchema

            class UsersHandler:
                def get(self):
                    '''Get users endpoint.
                    ---
                    description: Get a list of users
                    responses:
                        200:
                            description: A list of user
                            schema:
                                type: array
                                items: UserSchema
                    '''
                    users = Users.all()
                    schema = UserSchema(many=True)
                    return schema.dumps(users)

            urlspec = (r'/users', UsersHandler)
            spec.add_path(urlspec=urlspec)
            pprint(spec.to_dict()['paths'])
            # {'/users': {'get': {'description': 'Get a list of users',
            #                     'responses': {200: {'description': 'A list of users',
            #                                         'schema': {'type': 'array',
            #                                                    'items': {'$ref': '#/definitions/User'}}}}}}}

        """
        operations = (
            kwargs.get('operations') or
            (view and utils.load_operations_from_docstring(view.__doc__))
        )
        if not operations:
            return None
        operations = operations.copy()
        return Path(operations=operations)

    def operation_helper(self, operations, **kwargs):
        for operation in operations.values():
            if not isinstance(operation, dict):
                continue
            if 'parameters' in operation:
                operation['parameters'] = self.resolve_parameters(operation['parameters'])
            if self.openapi_version.major >= 3:
                if 'requestBody' in operation:
                    self.resolve_schema_in_request_body(operation['requestBody'])
            for response in operation.get('responses', {}).values():
                self.resolve_schema(response)


# Deprecated interface
def setup(spec):
    """Setup for the plugin.

    .. deprecated:: 0.39.0
        Use MarshmallowPlugin class.
    """
    plugin = MarshmallowPlugin(schema_name_resolver=spec.schema_name_resolver)
    plugin.init_spec(spec)
    spec.plugins.append(plugin)
