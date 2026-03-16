# -*- coding: utf-8 -*-
import copy

import typing
from six import iteritems
from six import string_types

from bravado_core._compat import Mapping
from bravado_core.exception import SwaggerMappingError


if getattr(typing, 'TYPE_CHECKING', False):
    from bravado_core.spec import Spec
    from bravado_core._compat_typing import JSONDict


# 'object' and 'array' are omitted since this should really be read as
# "Swagger types that map to python primitives"
SWAGGER_PRIMITIVES = (
    'integer',
    'number',
    'string',
    'boolean',
    'null',
)


def has_default(swagger_spec, schema_object_spec):
    # type: (Spec, JSONDict) -> bool
    return 'default' in swagger_spec.deref(schema_object_spec)


def get_default(swagger_spec, schema_object_spec):
    # type: (Spec, JSONDict) -> typing.Any
    return swagger_spec.deref(schema_object_spec).get('default')


def is_required(swagger_spec, schema_object_spec):
    # type: (Spec, JSONDict) -> bool
    return swagger_spec.deref(schema_object_spec).get('required', False)


def has_format(swagger_spec, schema_object_spec):
    # type: (Spec, JSONDict) -> bool
    return 'format' in swagger_spec.deref(schema_object_spec)


def get_format(swagger_spec, schema_object_spec):
    # type: (Spec, JSONDict) -> typing.Any
    return swagger_spec.deref(schema_object_spec).get('format')


def is_param_spec(swagger_spec, schema_object_spec):
    # type: (Spec, JSONDict) -> bool
    return 'in' in swagger_spec.deref(schema_object_spec)


def is_prop_nullable(swagger_spec, schema_object_spec):
    # type: (Spec, JSONDict) -> bool
    return swagger_spec.deref(schema_object_spec).get('x-nullable', False)


def is_ref(spec):
    # type: (typing.Any) -> bool
    """ Check if the given spec is a Mapping and contains a $ref.

    FYI: This function gets called A LOT during unmarshalling and is_dict_like
    adds a bunch of extra time. It is faster to use a try/except than it is
    to call is_dict_like first for every spec input.

    :param spec: swagger object specification
    :rtype: boolean
    """
    try:
        return '$ref' in spec and is_dict_like(spec) and isinstance(spec['$ref'], string_types)
    except TypeError:
        return False


def is_dict_like(spec):
    # type: (typing.Any) -> bool
    """
    :param spec: swagger object specification in dict form
    :rtype: boolean
    """
    # Calling isinstance(spec, Mapping) is relatively slow. As this function
    # gets usually called with a dict type argument we optimize for that case
    # by executing a much cheaper isinstance(spec, dict) check before the more
    # expensive isinstance(spec, Mapping) check.
    return isinstance(spec, (dict, Mapping))


def is_list_like(spec):
    # type: (typing.Any) -> bool
    """
    :param spec: swagger object specification in dict form
    :rtype: boolean
    """
    return isinstance(spec, (list, tuple))


def get_spec_for_prop(
    swagger_spec,  # type: Spec
    object_spec,  # type: JSONDict
    object_value,  # type: typing.Any
    prop_name,  # type: typing.Text
    properties=None,  # type: typing.Optional[typing.Mapping[typing.Text, JSONDict]]
):
    # type: (...) -> typing.Optional[JSONDict]
    """Given a jsonschema object spec and value, retrieve the spec for the
     given property taking 'additionalProperties' into consideration.

    :param swagger_spec: Spec object
    :type swagger_spec: bravado_core.spec.Spec
    :param object_spec: spec for a jsonschema 'object' in dict form
    :param object_value: jsonschema object containing the given property. Only
        used in error message.
    :param prop_name: name of the property to retrieve the spec for
    :param properties: collapsed properties of the object

    :return: spec for the given property or None if no spec found
    :rtype: dict or None
    """
    deref = swagger_spec.deref

    if properties is None:
        properties = collapsed_properties(deref(object_spec), swagger_spec)
    prop_spec = properties.get(prop_name)

    # TODO: remove is_nullable support once https://github.com/Yelp/bravado-core/issues/335 is addressed
    if prop_spec is not None:
        result_spec = deref(prop_spec)
        # If the de-referenced specification is for a x-nullable property
        # then copy the spec and add the x-nullable property.
        # If in the future there are other attributes on the property that
        # modify a referenced schema, it can be done here (or rewrite
        # unmarshal to pass the unreferenced property spec as another arg).
        if 'x-nullable' in prop_spec and 'x-nullable' not in result_spec:
            result_spec = copy.deepcopy(result_spec)
            result_spec['x-nullable'] = prop_spec['x-nullable']
        return result_spec

    additional_props = deref(object_spec).get('additionalProperties', True)

    if isinstance(additional_props, bool):
        # no spec for additional properties to conform to - this is basically
        # a way to send pretty much anything across the wire as is.
        return None

    additional_props = deref(additional_props)
    if is_dict_like(additional_props):
        # spec that all additional props MUST conform to
        return additional_props

    raise SwaggerMappingError(
        "Don't know what to do with `additionalProperties` in spec {0} "
        "when inspecting value {1}".format(object_spec, object_value),
    )


def handle_null_value(swagger_spec, schema_object_spec):
    # type: (Spec, JSONDict) -> typing.Any
    """Handle a null value for the associated schema object spec. Checks the
     x-nullable attribute in the spec to see if it is allowed and returns None
     if so and raises an exception otherwise.

    :param swagger_spec: Spec object
    :type swagger_spec: bravado_core.spec.Spec
    :param schema_object_spec: dict
    :return: The default if there is a default value, None if the spec is nullable
    :raises: SwaggerMappingError if the spec is not nullable and no default exists
    """
    if has_default(swagger_spec, schema_object_spec):
        return get_default(swagger_spec, schema_object_spec)

    if is_prop_nullable(swagger_spec, schema_object_spec):
        return None

    raise SwaggerMappingError(
        'Spec {0} is a required value'.format(schema_object_spec),
    )


def collapsed_properties(model_spec, swagger_spec):
    # type: (JSONDict, Spec) -> typing.Mapping[typing.Text, JSONDict]
    """Processes model spec and outputs dictionary with attributes
    as the keys and attribute spec as the value for the model.

    This handles traversing any polymorphic models and the hierarchy
    of properties properly.

    :param model_spec: model specification (must be dereferenced already)
    :type model_spec: dict
    :param swagger_spec: :class:`bravado_core.spec.Spec`
    :returns: dict
    """

    properties = {}

    # properties may or may not be present
    if 'properties' in model_spec:
        for attr, attr_spec in iteritems(model_spec['properties']):
            properties[attr] = attr_spec

    # allOf may or may not be present
    if 'allOf' in model_spec:
        deref = swagger_spec.deref
        for item_spec in model_spec['allOf']:
            item_spec = deref(item_spec)
            more_properties = collapsed_properties(item_spec, swagger_spec)
            properties.update(more_properties)

    return properties


def collapsed_required(model_spec, swagger_spec):
    # type: (JSONDict, Spec) -> typing.Set[typing.Text]
    """
    Processes model spec and outputs a set of required properties for the model.

    This handles traversing any polymorphic models and the hierarchy
    of properties properly.

    :param model_spec: model specification (must be dereferenced already)
    :type model_spec: dict
    :param swagger_spec: :class:`bravado_core.spec.Spec`
    :returns: Set of required properties.
    """
    required = set(model_spec.get('required', []))

    if 'allOf' in model_spec:
        deref = swagger_spec.deref
        for item_spec in model_spec['allOf']:
            item_spec = deref(item_spec)
            required.update(collapsed_required(item_spec, swagger_spec))

    return required


def get_type_from_schema(swagger_spec, schema_object_spec):
    # type: (Spec, JSONDict) -> typing.Optional[typing.Text]
    try:
        return schema_object_spec['type']
    except KeyError:
        if swagger_spec.config['default_type_to_object'] or 'allOf' in schema_object_spec:
            return 'object'
        return None
