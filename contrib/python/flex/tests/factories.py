import factory

from flex.constants import (
    EMPTY,
    QUERY,
    STRING,
)
from flex.loading.schema import swagger_schema_validator
from flex.loading.definitions import definitions_validator
from flex.http import (
    Request,
    Response,
)


class RequestFactory(factory.Factory):
    url = 'http://www.example.com/'
    method = 'get'
    content_type = 'application/json'
    body = EMPTY
    request = None
    headers = factory.Dict({})

    class Meta:
        model = Request


class ResponseFactory(factory.Factory):
    url = 'http://www.example.com/'
    content_type = 'application/json'
    content = EMPTY
    status_code = 200
    headers = factory.Dict({})

    request = factory.SubFactory(
        RequestFactory, url=factory.SelfAttribute('..url'),
    )

    class Meta:
        model = Response


class Meta:
    model = dict


ParameterFactory = type(
    'ParameterFactory',
    (factory.Factory,),
    {
        'Meta': Meta,
        'in': QUERY,
        'name': 'id',
        'type': STRING,
    },
)


class ResponseDefinitionFactory(factory.Factory):
    description = "A Generated Response definition"

    class Meta:
        model = dict


class HeaderDefinitionFactory(factory.Factory):
    class Meta:
        model = dict


def RawSchemaFactory(**kwargs):
    kwargs.setdefault('swagger', '2.0')
    kwargs.setdefault('info', {'title': 'Test API', 'version': '0.0.1'})
    kwargs.setdefault('paths', {})

    return kwargs


def SchemaFactory(**kwargs):
    raw_schema = RawSchemaFactory(**kwargs)

    context = {'deferred_references': set()}
    definitions = definitions_validator(raw_schema, context=context)

    swagger_schema = swagger_schema_validator(raw_schema, context=definitions)

    return swagger_schema
