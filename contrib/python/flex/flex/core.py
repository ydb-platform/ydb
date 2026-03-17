from __future__ import unicode_literals

from six.moves import urllib_parse as urlparse
import os
import requests
from copy import deepcopy

import six
import json
import yaml

from flex._compat import Mapping
from flex.context_managers import ErrorDict
from flex.exceptions import ValidationError
from flex.loading.definitions import (
    definitions_validator,
)
from flex.loading.schema import (
    swagger_schema_validator,
)
from flex.loading.schema.paths.path_item.operation.responses.single.schema import (
    schema_validator,
)
from flex.http import (
    normalize_request,
    normalize_response,
)
from flex.validation.common import validate_object
from flex.validation.request import validate_request
from flex.validation.response import validate_response


def load_source(source):
    """
    Common entry point for loading some form of raw swagger schema.

    Supports:
        - python object (dictionary-like)
        - path to yaml file
        - path to json file
        - file object (json or yaml).
        - json string.
        - yaml string.
    """
    if isinstance(source, Mapping):
        return deepcopy(source)
    elif hasattr(source, 'read') and callable(source.read):
        raw_source = source.read()
    elif os.path.exists(os.path.expanduser(str(source))):
        with open(os.path.expanduser(str(source)), 'r') as source_file:
            raw_source = source_file.read()
    elif isinstance(source, six.string_types):
        parts = urlparse.urlparse(source)
        if parts.scheme and parts.netloc:
            response = requests.get(source)
            if isinstance(response.content, six.binary_type):
                raw_source = six.text_type(response.content, encoding='utf-8')
            else:
                raw_source = response.content
        else:
            raw_source = source

    try:
        try:
            return json.loads(raw_source)
        except ValueError:
            pass

        try:
            return yaml.safe_load(raw_source)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError):
            pass
    except NameError:
        pass

    raise ValueError(
        "Unable to parse `{0}`.  Tried yaml and json.".format(source),
    )


def parse(raw_schema):
    context = {
        'deferred_references': set(),
    }
    swagger_definitions = definitions_validator(raw_schema, context=context)

    swagger_schema = swagger_schema_validator(
        raw_schema,
        context=swagger_definitions,
    )
    return swagger_schema


def load(target):
    """
    Given one of the supported target formats, load a swagger schema into it's
    python representation.
    """
    raw_schema = load_source(target)
    return parse(raw_schema)


def validate(raw_schema, target=None, **kwargs):
    """
    Given the python representation of a JSONschema as defined in the swagger
    spec, validate that the schema complies to spec.  If `target` is provided,
    that target will be validated against the provided schema.
    """
    schema = schema_validator(raw_schema, **kwargs)
    if target is not None:
        validate_object(target, schema=schema, **kwargs)


def validate_api_request(schema, raw_request):
    request = normalize_request(raw_request)

    with ErrorDict():
        validate_request(request=request, schema=schema)


def validate_api_response(schema, raw_response, request_method='get', raw_request=None):
    """
    Validate the response of an api call against a swagger schema.
    """
    request = None
    if raw_request is not None:
        request = normalize_request(raw_request)

    response = None
    if raw_response is not None:
        response = normalize_response(raw_response, request=request)

    if response is not None:
        validate_response(
            response=response,
            request_method=request_method,
            schema=schema
        )


def validate_api_call(schema, raw_request, raw_response):
    """
    Validate the request/response cycle of an api call against a swagger
    schema.  Request/Response objects from the `requests` and `urllib` library
    are supported.
    """
    request = normalize_request(raw_request)

    with ErrorDict() as errors:
        try:
            validate_request(
                request=request,
                schema=schema,
            )
        except ValidationError as err:
            errors['request'].add_error(err.messages or getattr(err, 'detail'))
            return

        response = normalize_response(raw_response, raw_request)

        try:
            validate_response(
                response=response,
                request_method=request.method,
                schema=schema
            )
        except ValidationError as err:
            errors['response'].add_error(err.messages or getattr(err, 'detail'))
