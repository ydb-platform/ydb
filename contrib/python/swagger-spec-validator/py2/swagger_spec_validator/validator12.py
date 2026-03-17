# -*- coding: utf-8 -*-
"""
Validate Swagger Specs against the Swagger 1.2 Specification.  The
validator aims to check for full compliance with the Specification.

The validator uses the published jsonschema files for basic structural
validation, augmented with custom validation code where necessary.

https://github.com/swagger-api/swagger-spec/blob/master/versions/1.2.md
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os

import jsonschema
import six
from jsonschema import RefResolver
from six.moves.urllib.parse import urlparse

from swagger_spec_validator.common import get_uri_from_file_path
from swagger_spec_validator.common import read_resource_file
from swagger_spec_validator.common import read_url
from swagger_spec_validator.common import SwaggerValidationError
from swagger_spec_validator.common import wrap_exception
from swagger_spec_validator.ref_validators import default_handlers

log = logging.getLogger(__name__)

# Primitives (§4.3.1)
PRIMITIVE_TYPES = ['integer', 'number', 'string', 'boolean']


def get_model_ids(api_declaration):
    models = api_declaration.get('models', {})
    return [model['id'] for model in six.itervalues(models)]


def get_resource_path(url, resource):
    """Fetch the complete resource path to get the api declaration.

    :param url: A file or http uri hosting the resource listing.
    :type url: string
    :param resource: Resource path starting with a '/'. eg. '/pet'
    :type resource: string
    :returns: Complete resource path hosting the api declaration.
    """
    if urlparse(url).scheme == 'file':
        parent_dir = os.path.dirname(url)

        def resource_file_name(resource):
            assert resource.startswith('/')
            return resource[1:] + '.json'
        path = os.path.join(parent_dir, resource_file_name(resource))
    else:
        path = url + resource

    return path


@wrap_exception
def validate_spec_url(url):
    """Simple utility function to perform recursive validation of a Resource
    Listing and all associated API Declarations.

    This is trivial wrapper function around
    :py:func:`swagger_spec_validator.validate_resource_listing` and
    :py:func:`swagger_spec_validator.validate_api_declaration`.  You are
    encouraged to write your own version of this if required.

    :param url: the URL of the Resource Listing.

    :returns: `None` in case of success, otherwise raises an exception.

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    """

    log.info('Validating %s', url)
    validate_spec(read_url(url), url)


def validate_spec(resource_listing, url):
    """
    Validates the resource listing, fetches the api declarations and
    consequently validates them as well.

    :type resource_listing: dict
    :param url: url serving the resource listing; needed to resolve api
                declaration path.
    :type url: string

    :returns: `None` in case of success, otherwise raises an exception.

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    """
    validate_resource_listing(resource_listing)

    for api in resource_listing['apis']:
        path = get_resource_path(url, api['path'])
        log.info('Validating %s', path)
        validate_api_declaration(read_url(path))


def validate_data_type(obj, model_ids, allow_arrays=True, allow_voids=False,
                       allow_refs=True, allow_file=False):
    """Validate an object that contains a data type (§4.3.3).

    Params:
    - obj: the dictionary containing the data type to validate
    - model_ids: a list of model ids
    - allow_arrays: whether an array is permitted in the data type.  This is
      used to prevent nested arrays.
    - allow_voids: whether a void type is permitted.  This is used when
      validating Operation Objects (§5.2.3).
    - allow_refs: whether '$ref's are permitted.  If true, then 'type's
      are not allowed to reference model IDs.
    """

    typ = obj.get('type')
    ref = obj.get('$ref')

    # TODO Use a custom jsonschema.Validator to Validate defaultValue
    # enum, minimum, maximum, uniqueItems
    if typ is not None:
        if typ in PRIMITIVE_TYPES:
            return
        if allow_voids and typ == 'void':
            return
        if typ == 'array':
            if not allow_arrays:
                raise SwaggerValidationError('"array" not allowed')
            # Items Object (§4.3.4)
            items = obj.get('items')
            if items is None:
                raise SwaggerValidationError('"items" not found')
            validate_data_type(items, model_ids, allow_arrays=False)
            return
        if typ == 'File':
            if not allow_file:
                raise SwaggerValidationError(
                    'Type "File" is only valid for form parameters')
            return
        if typ in model_ids:
            if allow_refs:
                raise SwaggerValidationError('must use "$ref" for referencing "%s"' % typ)
            return
        raise SwaggerValidationError('unknown type "%s"' % typ)

    if ref is not None:
        if not allow_refs:
            raise SwaggerValidationError('"$ref" not allowed')
        if ref not in model_ids:
            raise SwaggerValidationError('unknown model id "%s"' % ref)
        return

    raise SwaggerValidationError('no "$ref" or "type" present')


def validate_model(model, model_name, model_ids):
    """Validate a Model Object (§5.2.7)."""
    # TODO Validate 'sub-types' and 'discriminator' fields
    for required in model.get('required', []):
        if required not in model['properties']:
            raise SwaggerValidationError(
                'Model "%s": required property "%s" not found' %
                (model_name, required))

    if model_name != model['id']:
        error = 'model name: {} does not match model id: {}'.format(model_name, model['id'])
        raise SwaggerValidationError(error)

    for prop_name, prop in six.iteritems(model.get('properties', {})):
        try:
            validate_data_type(prop, model_ids, allow_refs=True)
        except SwaggerValidationError as e:
            # Add more context to the exception and re-raise
            raise SwaggerValidationError(
                'Model "{}", property "{}": {}'.format(model_name, prop_name, str(e)))


def validate_parameter(parameter, model_ids):
    """Validate a Parameter Object (§5.2.4)."""
    allow_file = parameter.get('paramType') == 'form'
    validate_data_type(
        parameter, model_ids, allow_refs=False, allow_file=allow_file)


def validate_operation(operation, model_ids):
    """Validate an Operation Object (§5.2.3)."""
    try:
        validate_data_type(operation, model_ids, allow_refs=False, allow_voids=True)
    except SwaggerValidationError as e:
        raise SwaggerValidationError(
            'Operation "{}": {}'.format(operation['nickname'], str(e)))

    for parameter in operation['parameters']:
        try:
            validate_parameter(parameter, model_ids)
        except SwaggerValidationError as e:
            raise SwaggerValidationError(
                'Operation "%s", parameter "%s": %s' %
                (operation['nickname'], parameter['name'], str(e)))


def validate_api(api, model_ids):
    """Validate an API Object (§5.2.2)."""
    for operation in api['operations']:
        validate_operation(operation, model_ids)


def validate_api_declaration(api_declaration):
    """Validate an API Declaration (§5.2).

    :param api_declaration: a dictionary respresentation of an API Declaration.

    :returns: `None` in case of success, otherwise raises an exception.

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    :raises: :py:class:`jsonschema.exceptions.ValidationError`
    """
    validate_json(api_declaration, 'schemas/v1.2/apiDeclaration.json')

    model_ids = get_model_ids(api_declaration)

    for api in api_declaration['apis']:
        validate_api(api, model_ids)

    for model_name, model in six.iteritems(api_declaration.get('models', {})):
        validate_model(model, model_name, model_ids)


def validate_resource_listing(resource_listing):
    """Validate a Resource Listing (§5.1).

    :param resource_listing: a dictionary respresentation of a Resource Listing.

    Note that you will have to invoke `validate_api_declaration` on each
    linked API Declaration.

    :returns: `None` in case of success, otherwise raises an exception.

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    :raises: :py:class:`jsonschema.exceptions.ValidationError`
    """
    validate_json(resource_listing, 'schemas/v1.2/resourceListing.json')


@wrap_exception
def validate_json(json_document, schema_path):
    """Validate a json document against a json schema.

    :param json_document: json document in the form of a list or dict.
    :param schema_path: package relative path of the json schema file.
    """
    schema, schema_path = read_resource_file(schema_path)

    resolver = RefResolver(
        base_uri=get_uri_from_file_path(schema_path),
        referrer=schema,
        handlers=default_handlers,
    )
    jsonschema.validate(json_document, schema, resolver=resolver)
