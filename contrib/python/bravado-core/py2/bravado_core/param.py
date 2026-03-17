# -*- coding: utf-8 -*-
import logging
from functools import partial

import simplejson as json
import six
import typing
from six.moves.urllib.parse import quote

from bravado_core import schema
from bravado_core.content_type import APP_JSON
from bravado_core.exception import SwaggerMappingError
from bravado_core.marshal import marshal_schema_object
from bravado_core.unmarshal import unmarshal_schema_object
from bravado_core.validate import validate_schema_object


if getattr(typing, 'TYPE_CHECKING', False):
    from bravado_core.operation import Operation
    from bravado_core.spec import Spec
    from bravado_core._compat_typing import JSONDict


log = logging.getLogger(__name__)

# 'multi' left out intentionally - http client lib should handle it
COLLECTION_FORMATS = {
    'csv': ',',
    'ssv': ' ',
    'tsv': '\t',
    'pipes': '|',
}


def stringify_body(value):
    """Json dump the value to string if not already in string
    """
    if not value or isinstance(value, six.string_types):
        return value
    return json.dumps(value)


class Param(object):
    """Thin wrapper around a param_spec dict that provides convenience functions
    for commonly requested parameter information.

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :type op: :class:`bravado_core.operation.Operation`
    :type param_spec: parameter specification in dict form
    """

    def __init__(self, swagger_spec, op, param_spec):
        # type: (Spec, Operation, JSONDict) -> None
        self.op = op
        self.swagger_spec = swagger_spec
        self.param_spec = swagger_spec.deref(param_spec)

    @property
    def name(self):
        return self.param_spec['name']

    @property
    def location(self):
        # not using 'in' as the name since it is a keyword in python
        return self.param_spec['in']

    @property
    def description(self):
        return self.param_spec.get('description')

    @property
    def required(self):
        return self.param_spec.get('required', False)

    def has_default(self):
        return self.default is not None

    @property
    def default(self):
        return self.param_spec.get('default')


def get_param_type_spec(param):
    """The spec for the parameter 'type' is not always in the same place for a
    parameter. The notable exception is when the location is 'body' and the
    spec for the type is in param_spec['schema']

    :type param: :class:`bravado_core.param.Param`

    :rtype: dict
    :returns: the param spec that contains 'type'
    :raises: SwaggerMappingError when param location is not valid
    """
    location = param.location
    if location in ('path', 'query', 'header', 'formData'):
        return param.param_spec
    if location == 'body':
        return param.swagger_spec.deref(param.param_spec).get('schema')
    raise SwaggerMappingError(
        "Don't know how to handle location {0} in parameter {1}".format(location, param),
    )


def marshal_param(param, value, request):
    """Given an operation's parameter and its value, marshal the value and
    place it in the proper request destination.

    Destination is one of:
        - path - can accept primitive and array of primitive types
        - query - can accept primitive and array of primitive types
        - header - can accept primitive and array of primitive types
        - body - can accept any type
        - formData - can accept primitive and array of primitive types

    :type param: :class:`bravado_core.param.Param`
    :param value: The value to assign to the parameter
    :type request: dict
    """
    swagger_spec = param.swagger_spec
    deref = swagger_spec.deref

    param_spec = deref(get_param_type_spec(param))
    location = param.location

    # Rely on unmarshalling behavior on the other side of the pipe to use
    # the default value if one is available.
    if value is None and not param.required:
        return

    value = marshal_schema_object(swagger_spec, param_spec, value)

    if swagger_spec.config['validate_requests']:
        validate_schema_object(swagger_spec, param_spec, value)

    param_type = param_spec.get('type')
    if param_type == 'array' and location != 'body':
        value = marshal_collection_format(swagger_spec, param_spec, value)

    encode_param = partial(encode_request_param, param_type, param.name)
    if location == 'path':
        token = u'{%s}' % param.name
        quoted_value = quote(six.text_type(value).encode('utf8'), safe=',')
        request['url'] = request['url'].replace(token, quoted_value)
    elif location == 'query':
        request['params'][param.name] = encode_param(value)
    elif location == 'header':
        request['headers'][param.name] = encode_param(value)
    elif location == 'formData':
        if param_type == 'file':
            add_file(param, value, request)
        else:
            request.setdefault('data', {})[param.name] = value
    elif location == 'body':
        request['headers']['Content-Type'] = APP_JSON
        request['data'] = json.dumps(value)
    else:
        raise SwaggerMappingError(
            "Don't know how to marshal_param with location {0}".format(location),
        )


def unmarshal_param(param, request):
    """Unmarshal the given parameter from the passed in request like object.

    :type param: :class:`bravado_core.param.Param`
    :type request: :class:`bravado_core.request.IncomingRequest`
    :return: value of parameter
    """
    swagger_spec = param.swagger_spec
    deref = swagger_spec.deref
    param_spec = deref(get_param_type_spec(param))
    location = param.location
    param_type = deref(param_spec.get('type'))
    cast_param = partial(cast_request_param, param_type, param.name)

    default_value = schema.get_default(swagger_spec, param_spec)

    if location == 'path':
        raw_value = cast_param(request.path.get(param.name, None))
    elif location == 'query':
        raw_value = cast_param(request.query.get(param.name, default_value))
    elif location == 'header':
        raw_value = cast_param(request.headers.get(param.name, default_value))
    elif location == 'formData':
        if param_type == 'file':
            raw_value = request.files.get(param.name, None)
        else:
            raw_value = cast_param(request.form.get(param.name, default_value))
    elif location == 'body':
        try:
            # TODO: verify content-type header
            raw_value = request.json()
        except ValueError as json_error:
            # If the body parameter is required then we should make sure that an exception
            # is thrown, instead if the body parameter is optional is OK-ish to assume that
            # raw_value is the default_value
            if param.required:
                raise SwaggerMappingError(
                    "Error reading request body JSON: {0}".format(str(json_error)),
                )
            else:
                raw_value = default_value
    else:
        raise SwaggerMappingError(
            "Don't know how to unmarshal_param with location {0}".format(location),
        )

    if raw_value is None and not param.required:
        return None

    if param_type == 'array' and location != 'body':
        raw_value = unmarshal_collection_format(
            swagger_spec, param_spec,
            raw_value,
        )

    if swagger_spec.config['validate_requests']:
        validate_schema_object(swagger_spec, param_spec, raw_value)

    value = unmarshal_schema_object(swagger_spec, param_spec, raw_value)
    return value


def string_to_boolean(value):
    """Coerce the provided value into its Python boolean value if it's a string
    or return the value as-is if already casted.

    :param value: the value of a Swagger parameter with a boolean type
    :type value: usually string, but sometimes a bool
    """
    if isinstance(value, bool):
        return value

    lowercase_value = value.lower()
    true = ['true', '1']
    false = ['false', '0']
    if lowercase_value in true:
        return True
    if lowercase_value in false:
        return False
    # Failed casts raise a ValueError
    raise ValueError()


CAST_TYPE_TO_FUNC = {
    # Values come in as strings, these functions try to
    # cast them to the right type
    'integer': int,
    'number': float,
    'boolean': string_to_boolean,
}


def cast_request_param(param_type, param_name, param_value):
    """Try to cast a request param (e.g. query arg, POST data) from a string to
    its specified type in the schema. This allows validating non-string params.

    :param param_type: name of the type to be casted to
    :type  param_type: string
    :param param_name: param name
    :type  param_name: string
    :param param_value: param value
    :type  param_value: string
    """
    if param_value is None:
        return None

    if param_type in CAST_TYPE_TO_FUNC and param_value == '':
        return None

    try:
        return CAST_TYPE_TO_FUNC.get(param_type, lambda x: x)(param_value)
    except (ValueError, TypeError):
        log.warning(
            "Failed to cast %s value of %s to %s",
            param_name, param_value, param_type,
        )
        # Ignore type error, let jsonschema validation handle incorrect types
        return param_value


def encode_request_param(param_type, param_name, param_value):
    """
    Tries to cast a request param from its specified type in scheme to a string.
    This allows passing non-string params in query arg, POST data, etc.

    :param param_type: name of the type to be casted to
    :type  param_type: string
    :param param_name: param name
    :type  param_name: string
    :param param_value: param value
    """
    if param_type == 'array':
        # marshal_collection_format has already taken care of it; furthermore, if collectionFormat is
        # multi then the value is still a sequence, we don't want to cast that to str
        return param_value

    if param_value is None:  # pragma: no cover
        # We should never get into this branch, but better be more defensive
        return None

    try:
        param_value = str(param_value)
        if param_type == 'boolean':
            param_value = param_value.lower()
        return param_value
    except (ValueError, TypeError):  # pragma: no cover
        # Conversion to string should never fail, but as unicode issues are always
        # a thing in python I would rather prefer logging the issue instead of
        # raising the exception
        log.warning(
            "Failed to encode %s value of %s from type %s to string",
            param_name, param_value, param_type,
        )
        # Ignore type error, let jsonschema validation handle incorrect types
        return param_value


def add_file(param, value, request):
    """Add a parameter of type 'file' to the given request.

    :type param: :class;`bravado_core.param.Param`
    :param value: The raw content of the file to be uploaded
    :type request: dict
    """
    if request.get('files') is None:
        # support multiple files by default by setting to an empty array
        request['files'] = []

        # The http client should take care of setting the content-type header
        # to 'multipart/form-data'. Just verify that the swagger spec is
        # conformant
        expected_mime_type = 'multipart/form-data'

        # TODO: Remove after https://github.com/Yelp/swagger_spec_validator/issues/22 is implemented  # noqa
        if expected_mime_type not in param.op.consumes:
            raise SwaggerMappingError(
                "Mime-type '{0}' not found in list of supported mime-types for "
                "parameter '{1}' on operation '{2}': {3}".format(
                    expected_mime_type,
                    param.name,
                    param.op.operation_id,
                    param.op.consumes,
                ),
            )

    if isinstance(value, tuple):
        filename, val = value
    else:
        filename, val = param.name, value

    file_tuple = (param.name, (filename, val))
    request['files'].append(file_tuple)


def marshal_collection_format(swagger_spec, param_spec, value):
    """For an array, apply the collection format and return the result.

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :param param_spec: spec of the parameter with 'type': 'array'
    :param value: array value of the parameter

    :return: transformed value as a string
    """
    collection_format = swagger_spec.deref(param_spec).get('collectionFormat', 'csv')

    if collection_format == 'multi':
        # http client lib should handle this
        return value

    sep = COLLECTION_FORMATS[collection_format]
    return sep.join(str(element) for element in value)


def unmarshal_collection_format(swagger_spec, param_spec, value):
    """For a non-body parameter of type array, unmarshal the value into an
    array of elements.

    Input:
        param_spec = {
            'name': 'status'
            'in': 'query',
            'collectionFormat': 'psv', # pipe separated value
            'type': 'array',
            'items': {
                'type': 'string',
            }
        }
        value="pending|completed|started"

    Output:
        ['pending', 'completed', 'started']

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :param param_spec: param_spec of the parameter with 'type': 'array'
    :type param_spec: dict
    :param value: parameter value
    :type value: string

    :rtype: list
    """
    deref = swagger_spec.deref
    param_spec = deref(param_spec)
    collection_format = param_spec.get('collectionFormat', 'csv')

    if value is None:
        if not schema.is_required(swagger_spec, param_spec):
            # Just pass through an optional array that has no value
            return None
        return schema.handle_null_value(swagger_spec, param_spec)

    if schema.is_list_like(value):
        value_array = value
    elif collection_format == 'multi':
        # http client lib should have already unmarshalled the value
        value_array = [value]
    else:
        sep = COLLECTION_FORMATS[collection_format]
        if value == '':
            value_array = []
        else:
            value_array = value.split(sep)

    items_spec = param_spec['items']
    items_type = deref(items_spec).get('type')
    param_name = param_spec['name']

    return [
        cast_request_param(items_type, param_name, item)
        for item in value_array
    ]
