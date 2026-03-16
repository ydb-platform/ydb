# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import functools
import json
import logging
import string
import warnings

from jsonschema.validators import Draft4Validator
from jsonschema.validators import RefResolver
from six import iteritems
from six import iterkeys
from six import string_types

from swagger_spec_validator import ref_validators
from swagger_spec_validator.common import get_uri_from_file_path
from swagger_spec_validator.common import read_resource_file
from swagger_spec_validator.common import read_url
from swagger_spec_validator.common import SwaggerValidationError
from swagger_spec_validator.common import SwaggerValidationWarning
from swagger_spec_validator.common import wrap_exception
from swagger_spec_validator.ref_validators import default_handlers
from swagger_spec_validator.ref_validators import in_scope
from swagger_spec_validator.ref_validators import validate_schema_value


log = logging.getLogger(__name__)


def validate_ref(ref_dict, path):
    """Check if a ref_dict has siblings that will be overwritten by $ref or $ref is None.

    While the siblings case does not contradict the spec, it may cause confusion and mislead
    developers. See https://stackoverflow.com/a/48114924.

    :param ref_dict: A dict that may be {'$ref': '#/blah/blah', 'x-nullable': true}.
    :type ref_dict: dict

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    """
    keys_to_ignore = {'x-scope', '$ref', 'description'}

    if any(key for key in iterkeys(ref_dict) if key not in keys_to_ignore):
        warnings.warn(
            SwaggerValidationWarning(
                'Found "$ref: {}" with siblings that will be overwritten. '
                'See https://stackoverflow.com/a/48114924 for more information. (path {})'.format(
                    ref_dict['$ref'], '/'.join(path),
                )
            ),
        )

    if ref_dict['$ref'] is None:
        warnings.warn(
            SwaggerValidationWarning(
                'Identified $ref with None value. This is usually an error, '
                'although technically it might be allowed. (path: {})'.format('/'.join(path)),
            ),
        )


def validate_references(raw_spec, deref, path=None, visited_spec_ids=None):
    if path is None:
        path = ['#']

    if visited_spec_ids is None:
        visited_spec_ids = set()

    if id(raw_spec) in visited_spec_ids:
        return

    # Register raw_spec into visited_spec_ids to prevent unbounded recursion
    visited_spec_ids.add(id(raw_spec))

    if isinstance(raw_spec, dict) and '$ref' in raw_spec:
        # The additional check is needed as `is_ref` will consider raw_spec dictionaries with `$ref`
        # attribute and string value.
        # The goal of validate_ref is to ensure that objects that looks like a reference is actually
        # valid by warning users about those cases.
        # Due to _looks like_ we need to almost duplicate the check by removing the string enforcement
        validate_ref(ref_dict=raw_spec, path=path)

    if is_ref(raw_spec) and 'x-scope' in raw_spec:
        # Ensure that we're following references only if they were already
        # descended by jsonschema during initial schema validation (x-scope is present)
        # This is mostly done to ensure that we're not causing RefResolutionError
        # due to the fact that the reference is relative and we have no information
        # about the base path.
        # This could be handled differently but we thought that checking references that
        # were actively part of the specs is a valid trade-off here
        validate_references(deref(raw_spec), deref, path, visited_spec_ids)
    elif isinstance(raw_spec, dict):
        for k, v in sorted(iteritems(raw_spec)):
            validate_references(v, deref, path + [k], visited_spec_ids)
    elif isinstance(raw_spec, list):
        for k, v in enumerate(raw_spec):
            validate_references(v, deref, path + [str(k)], visited_spec_ids)


def deref(ref_dict, resolver):
    """Dereference ref_dict (if it is indeed a ref) and return what the
    ref points to.

    :param ref_dict: Something like {'$ref': '#/blah/blah'}
    :type ref_dict: dict
    :param resolver: Ref resolver used to do the de-referencing
    :type resolver: :class:`jsonschema.RefResolver`

    :return: de-referenced value of ref_dict
    :rtype: scalar, list, dict
    """
    if ref_dict is None or not is_ref(ref_dict):
        return ref_dict

    ref = ref_dict['$ref']
    with in_scope(resolver, ref_dict):
        with resolver.resolving(ref) as target:
            log.debug('Resolving %s', ref)
            return target


@wrap_exception
def validate_spec_url(spec_url):
    """Validates a Swagger 2.0 API Specification at the given URL.

    :param spec_url: the URL of the service's swagger spec.

    :returns: The resolver (with cached remote refs) used during validation
    :rtype: :class:`jsonschema.RefResolver`
    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    """
    log.info('Validating %s', spec_url)
    return validate_spec(read_url(spec_url), spec_url)


def validate_spec(spec_dict, spec_url='', http_handlers=None):
    """Validates a Swagger 2.0 API Specification given a Swagger Spec.

    :param spec_dict: the json dict of the swagger spec.
    :type spec_dict: dict
    :param spec_url: url from which spec_dict was retrieved. Used for
        dereferencing refs. eg: file:///foo/swagger.json
    :type spec_url: string
    :param http_handlers: used to download any remote $refs in spec_dict with
        a custom http client. Defaults to None in which case the default
        http client built into jsonschema's RefResolver is used. This
        is a mapping from uri scheme to a callable that takes a
        uri.

    :returns: the resolver (with cached remote refs) used during validation
    :rtype: :class:`jsonschema.RefResolver`
    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    """
    swagger_resolver = validate_json(
        spec_dict,
        'schemas/v2.0/schema.json',
        spec_url=spec_url,
        http_handlers=http_handlers,
    )

    bound_deref = functools.partial(deref, resolver=swagger_resolver)
    spec_dict = bound_deref(spec_dict)
    apis = bound_deref(spec_dict['paths'])
    definitions = bound_deref(spec_dict.get('definitions', {}))
    validate_apis(apis, bound_deref)
    validate_definitions(definitions, bound_deref)
    validate_parameters(bound_deref(spec_dict.get('parameters', {})), bound_deref)
    validate_references(spec_dict, bound_deref)
    return swagger_resolver


@wrap_exception
def validate_json(spec_dict, schema_path, spec_url='', http_handlers=None):
    """Validate a json document against a json schema.

    :param spec_dict: json document in the form of a list or dict.
    :param schema_path: package relative path of the json schema file.
    :param spec_url: base uri to use when creating a
        RefResolver for the passed in spec_dict.
    :param http_handlers: used to download any remote $refs in spec_dict with
        a custom http client. Defaults to None in which case the default
        http client built into jsonschema's RefResolver is used. This
        is a mapping from uri scheme to a callable that takes a
        uri.

    :return: RefResolver for spec_dict with cached remote $refs used during
        validation.
    :rtype: :class:`jsonschema.RefResolver`
    """
    schema, schema_path = read_resource_file(schema_path)

    schema_resolver = RefResolver(
        base_uri=get_uri_from_file_path(schema_path),
        referrer=schema,
        handlers=default_handlers,
    )

    spec_resolver = RefResolver(
        base_uri=spec_url,
        referrer=spec_dict,
        handlers=http_handlers or default_handlers,
    )

    ref_validators.validate(
        instance=spec_dict,
        schema=schema,
        resolver=schema_resolver,
        instance_cls=ref_validators.create_dereffing_validator(spec_resolver),
        cls=Draft4Validator,
    )

    # Since remote $refs were downloaded, pass the resolver back to the caller
    # so that its cached $refs can be re-used.
    return spec_resolver


@wrap_exception
def validate_value_type(schema, value, deref):
    # Extract resolver from deref partial build on ``validate_spec``
    # This is used in order to use already fetched external references
    # If it is missing a new RefResolver will be initialized
    swagger_resolver = getattr(deref, 'keywords', {}).get('resolver', None)
    validate_schema_value(schema=deref(schema), value=value, swagger_resolver=swagger_resolver)


def validate_default_in_parameter(param_spec, deref):
    deref_param_spec = deref(param_spec)

    if deref_param_spec.get('required'):
        # If the parameter is a required parameter, default has no meaning
        return

    if 'default' in deref_param_spec:
        if deref_param_spec['default'] is None and deref_param_spec.get('x-nullable', False) is True:
            # In case x-nullable property is set to true, null is a valid default
            return
        validate_value_type(
            schema=deref_param_spec,
            value=deref_param_spec['default'],
            deref=deref,
        )


def validate_defaults_in_parameters(params_spec, deref):
    """
    Validates that default values for api parameters are
    of the parameter type

    :param params_spec: list of parameter objects (#/paths/<path>/<http_verb>/parameters)
    :param deref: callable that dereferences $refs

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    """
    # Note: this functions is preserved to avoid public signature updates (it's not used internally)
    for param_spec in params_spec:
        validate_default_in_parameter(param_spec, deref)


def validate_responses(api, http_verb, responses_dict, deref=None):
    if is_ref(responses_dict):
        raise SwaggerValidationError(
            '{http_verb} {api} does not have a valid responses section. '
            'That section cannot be just a reference to another object.'.format(
                http_verb=http_verb.upper(),
                api=api,
            )
        )
    for response_status, response_spec in iteritems(responses_dict):
        response_schema = response_spec.get('schema')
        if response_schema is None:
            continue
        validate_definition(
            definition=response_schema,
            deref=deref,
            def_name='#/paths/{api}/{http_verb}/responses/{status_code}'.format(
                http_verb=http_verb,
                api=api,
                status_code=response_status,
            ),
            visited_definitions=set(),
        )


def validate_non_body_parameter(param, deref, def_name):
    if 'type' not in param:
        raise SwaggerValidationError(
            'Non-Body parameter in `{def_name}` does not specify `type`.'.format(def_name=def_name),
        )

    if param['type'] == 'array' and 'items' not in param:
        raise SwaggerValidationError(
            'Non-Body array parameter in `{def_name}` does not specify `items`.'.format(def_name=def_name),
        )


def validate_body_parameter(param, deref, def_name):
    if 'schema' not in param:
        raise SwaggerValidationError(
            'Body parameter in `{def_name}` does not specify `schema`.'.format(def_name=def_name)
        )

    validate_definition(
        definition=param['schema'],
        deref=deref,
        def_name='{}/schema'.format(def_name),
        visited_definitions=set(),
    )


def validate_parameter(param, deref, def_name):
    validate_default_in_parameter(param, deref)

    if not is_ref(param):
        if param['in'] == 'body':
            validate_body_parameter(param, deref, def_name)
        else:
            validate_non_body_parameter(param, deref, def_name)


def validate_apis(apis, deref):
    """Validates semantic errors in #/paths.

    :param apis: dict of all the #/paths
    :param deref: callable that dereferences $refs

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    :raises: :py:class:`jsonschema.exceptions.ValidationError`
    """
    operation_id_set = set()

    for api_name, api_body in iteritems(apis):
        api_body = deref(api_body)
        api_params = deref(api_body.get('parameters', []))
        validate_duplicate_param(api_params, deref)
        for idx, param in enumerate(api_params):
            validate_parameter(
                param=param,
                deref=deref,
                def_name='#/paths/{api_name}/parameters/{idx}'.format(
                    api_name=api_name,
                    idx=idx,
                ),
            )

        for oper_name in api_body:
            # don't treat parameters that apply to all api operations as
            # an operation
            if oper_name == 'parameters' or oper_name.startswith('x-'):
                continue
            oper_body = deref(api_body[oper_name])

            # Check that, if this operation has an operationId defined,
            # no other operation also has that operationId.
            operation_id = oper_body.get('operationId')
            if operation_id is not None:
                if operation_id in operation_id_set:
                    raise SwaggerValidationError(
                        "Duplicate operationId: {}".format(operation_id)
                    )
                operation_id_set.add(operation_id)

            oper_params = deref(oper_body.get('parameters', []))
            validate_duplicate_param(oper_params, deref)
            all_path_params = list(set(
                get_path_param_names(api_params, deref) + get_path_param_names(oper_params, deref),
            ))
            validate_unresolvable_path_params(api_name, all_path_params)
            for idx, param in enumerate(oper_params):
                validate_parameter(
                    param=param,
                    deref=deref,
                    def_name='#/paths/{api_name}/{oper_name}/parameters/{idx}'.format(
                        api_name=api_name,
                        oper_name=oper_name,
                        idx=idx,
                    ),
                )
            # Responses validation
            validate_responses(api_name, oper_name, oper_body['responses'], deref)


def get_collapsed_properties_type_mappings(definition, deref):
    """
    Get all the properties for a swagger model (definition).
    :param definition: dictionary representation of the definition
    :type definition: dict
    :param deref: callable that dereferences $refs
    :return: (required properties type mapping, not required properties type mapping)
    :type: tuple
    """
    definition = deref(definition)
    required_properties = {}
    not_required_properties = {}
    if definition.get('allOf'):
        for inner_definition in definition['allOf']:
            inner_required_properties, inner_not_required_properties = get_collapsed_properties_type_mappings(inner_definition, deref)
            required_properties.update(inner_required_properties)
            not_required_properties.update(inner_not_required_properties)
    else:
        properties = {
            prop_name: prop_schema.get('type', 'object')
            for prop_name, prop_schema in iteritems(definition.get('properties', {}))
        }
        required_properties_set = set(definition.get('required', []))
        for k, v in iteritems(properties):
            if k in required_properties_set:
                required_properties[k] = v
            else:
                not_required_properties[k] = v

    return required_properties, not_required_properties


def validate_property_default(property_spec, deref):
    """
    Validates that default values for definitions are of the property type.
    Enforces presence of "type" in case of "default" presence.

    :param property_spec: schema object (#/definitions/<def_name>/properties/<property_name>
    :param deref: callable that dereferences $refs

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    """
    deref_property_spec = deref(property_spec)
    if 'default' in deref_property_spec:
        if deref_property_spec['default'] is None and deref_property_spec.get('x-nullable', False) is True:
            # In case x-nullable property is set to true, null is a valid default
            return

        validate_value_type(schema=property_spec, value=deref_property_spec['default'], deref=deref)


def validate_defaults_in_definition(definition_spec, deref):
    for property_name, property_spec in iteritems(definition_spec.get('properties', {})):
        validate_property_default(property_spec, deref)


def validate_arrays_in_definition(definition_spec, deref, def_name=None, visited_definitions=None):
    if definition_spec.get('type') == 'array':
        if 'items' not in definition_spec:
            raise SwaggerValidationError(
                'Definition of type array must define `items` property{}.'.format(
                    '' if not def_name else ' (definition {})'.format(def_name),
                ),
            )
        validate_definition(
            definition=definition_spec['items'],
            deref=deref,
            def_name='{}/items'.format(def_name),
            visited_definitions=visited_definitions,
        )


def validate_definition(definition, deref, def_name=None, visited_definitions=None):
    """
    :param visited_definitions: set of already visited definitions
                                    This is used to cut recursion in case of recursive definitions
    :type visited_definitions: set
    """
    if visited_definitions is not None:
        # Remove x-scope or else no two definitions will be the same
        stripped_definition = json.dumps({key: definition[key] for key in definition if key != 'x-scope'}, sort_keys=True)
        if stripped_definition in visited_definitions:
            return
        visited_definitions.add(stripped_definition)

    definition = deref(definition)

    swagger_type = definition.get('type')
    if isinstance(swagger_type, list):
        # not valid Swagger; see https://github.com/OAI/OpenAPI-Specification/issues/458
        raise SwaggerValidationError('In definition of {}, type must be a string; lists are not allowed ({})'.format(def_name or '(no name)', swagger_type))

    if 'allOf' in definition:
        for idx, inner_definition in enumerate(definition['allOf']):
            validate_definition(
                definition=inner_definition,
                deref=deref,
                def_name='{}/{}'.format(def_name, str(idx)),
                visited_definitions=visited_definitions,
            )
    else:
        required = definition.get('required', [])
        props = iterkeys(definition.get('properties', {}))
        extra_props = list(set(required) - set(props))
        if extra_props:
            raise SwaggerValidationError(
                "In definition of {}, required list has properties not defined: {}.".format(
                    def_name or '(no name)', extra_props,
                )
            )

        validate_defaults_in_definition(definition, deref)
        validate_arrays_in_definition(
            definition_spec=definition,
            deref=deref,
            def_name=def_name,
            visited_definitions=visited_definitions
        )

        for property_name, property_spec in iteritems(definition.get('properties', {})):
            validate_definition(
                definition=property_spec,
                deref=deref,
                def_name='{}/properties/{}'.format(def_name, property_name),
                visited_definitions=visited_definitions,
            )

    if 'additionalProperties' in definition:
        if definition.get('additionalProperties') not in (True, False):
            validate_definition(
                definition=definition.get('additionalProperties'),
                deref=deref,
                def_name='{}/additionalProperties'.format(def_name),
                visited_definitions=visited_definitions,
            )

    if 'discriminator' in definition:
        required_props, not_required_props = get_collapsed_properties_type_mappings(definition, deref)
        discriminator = definition['discriminator']
        if discriminator not in required_props and discriminator not in not_required_props:
            raise SwaggerValidationError('In definition of {}, discriminator ({}) must be defined in properties'.format(def_name or '(no name)', discriminator))
        if discriminator not in required_props:
            raise SwaggerValidationError('In definition of {}, discriminator ({}) must be a required property'.format(def_name or '(no name)', discriminator))
        if required_props[discriminator] != 'string':
            raise SwaggerValidationError('In definition of {}, discriminator ({}) must be a string property'.format(def_name or '(no name)', discriminator))


def validate_definitions(definitions, deref):
    """Validates the semantic errors in #/definitions.

    :param definitions: dict of all the definitions
    :param deref: callable that dereferences $refs

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    :raises: :py:class:`jsonschema.exceptions.ValidationError`
    """
    visited_definitions = set()
    for def_name, definition in iteritems(definitions):
        validate_definition(
            definition=definition,
            deref=deref,
            def_name='#/definitions/{}'.format(def_name),
            visited_definitions=visited_definitions,
        )


def validate_parameters(parameters, deref):
    for param_name, param_spec in iteritems(parameters):
        validate_parameter(
            param=param_spec,
            deref=deref,
            def_name='#/parameters/{}'.format(param_name),
        )


def get_path_param_names(params, deref):
    """Fetch all the names of the path parameters of an operation.

    :param params: list of all the params
    :param deref: callable that dereferences $refs

    :returns: list of the name of the path params
    """
    return [
        deref(param)['name']
        for param in params
        if deref(param)['in'] == 'path'
    ]


def validate_duplicate_param(params, deref):
    """Validate no duplicate parameters are present.

    Uniqueness is determined by the tuple ('name', 'in').

    :param params: list of all the params
    :param deref: callable that dereferences $refs

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError` when
        a duplicate parameter is found.
    """
    seen = set()
    msg = "Duplicate param found with (name, in)"
    for param in params:
        param = deref(param)
        param_key = (param['name'], param['in'])
        if param_key in seen:
            raise SwaggerValidationError("{}: {}".format(msg, param_key))
        seen.add(param_key)


def get_path_params_from_url(path):
    """Parse the path parameters from a path string

    :param path: path url to parse for parameters

    :returns: List of path parameter names
    """
    formatter = string.Formatter()
    path_params = [item[1] for item in formatter.parse(path)]
    return filter(None, path_params)


def validate_unresolvable_path_params(path_name, path_params):
    """Validate that every path parameter listed is also defined.

    :param path_name: complete path name as a string.
    :param path_params: Names of all the eligible path parameters

    :raises: :py:class:`swagger_spec_validator.SwaggerValidationError`
    """
    for path in get_path_params_from_url(path_name):
        if path not in path_params:
            msg = "Path parameter '{}' used is not documented on '{}'".format(path, path_name)
            raise SwaggerValidationError(msg)


def is_ref(spec_dict):
    return isinstance(spec_dict, dict) and isinstance(spec_dict.get('$ref'), string_types)
