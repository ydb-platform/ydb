# -*- coding: utf-8 -*-
import warnings
from functools import partial

import typing
from six import iteritems

from bravado_core import _decorators
from bravado_core import schema
from bravado_core.exception import SwaggerMappingError
from bravado_core.model import Model
from bravado_core.model import MODEL_MARKER
from bravado_core.schema import collapsed_properties
from bravado_core.schema import collapsed_required
from bravado_core.schema import get_type_from_schema
from bravado_core.schema import is_dict_like
from bravado_core.schema import is_list_like
from bravado_core.schema import SWAGGER_PRIMITIVES
from bravado_core.util import memoize_by_id


if getattr(typing, 'TYPE_CHECKING', False):
    from bravado_core._compat_typing import JSONDict
    from bravado_core._compat_typing import MarshalingMethod
    from bravado_core._compat_typing import NoReturn
    from bravado_core.spec import Spec
    from bravado_core.formatter import SwaggerFormat


_NOT_FOUND = object()
_handle_null_value = partial(
    _decorators.handle_null_value,
    is_marshaling_operation=True,
)


def marshal_schema_object(swagger_spec, schema_object_spec, value):
    # type: (Spec, JSONDict, typing.Any) -> typing.Any
    """Marshal the value using the given schema object specification.

    Marshaling includes:
    - transform the value according to 'format' if available
    - return the value in a form suitable for 'on-the-wire' transmission

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :type schema_object_spec: dict
    :type value: int, long, string, unicode, boolean, list, dict, Model type

    :return: marshaled value
    :rtype: int, long, string, unicode, boolean, list, dict
    :raises: SwaggerMappingError
    """
    marshaling_method = _get_marshaling_method(swagger_spec=swagger_spec, object_schema=schema_object_spec)
    return marshaling_method(value)


def marshal_primitive(swagger_spec, primitive_spec, value):
    # type: (Spec, JSONDict, typing.Any) -> typing.Any
    """Marshal a python primitive type into a jsonschema primitive.

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :type primitive_spec: dict
    :type value: int, long, float, boolean, string, unicode, or an object
        based on 'format'

    :rtype: int, long, float, boolean, string, unicode, etc
    :raises: SwaggerMappingError
    """
    warnings.warn(
        'marshal_primitive will be deprecated in the next major release. '
        'Please use the more general entry-point offered in marshal_schema_object',
        DeprecationWarning,
    )

    null_decorator = _handle_null_value(
        swagger_spec=swagger_spec,
        object_schema=primitive_spec,
    )
    marshal_function = _marshaling_method_primitive_type(swagger_spec=swagger_spec, object_schema=primitive_spec)
    return null_decorator(marshal_function)(value)


def marshal_array(swagger_spec, array_spec, array_value):
    # type: (Spec, JSONDict, typing.Any) -> typing.Any
    """Marshal a jsonschema type of 'array' into a json-like list.

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :type array_spec: dict
    :type array_value: list
    :rtype: list
    :raises: SwaggerMappingError
    """
    warnings.warn(
        'marshal_array will be deprecated in the next major release. '
        'Please use the more general entry-point offered in marshal_schema_object',
        DeprecationWarning,
    )
    null_decorator = _handle_null_value(
        swagger_spec=swagger_spec,
        object_schema=array_spec,
    )
    marshal_function = _marshaling_method_array(swagger_spec=swagger_spec, object_schema=array_spec)
    return null_decorator(marshal_function)(array_value)


def marshal_object(swagger_spec, object_spec, object_value):
    # type: (Spec, JSONDict, typing.Any) -> typing.Any
    """Marshal a python dict to json dict.

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :type object_spec: dict
    :type object_value: dict

    :rtype: dict
    :raises: SwaggerMappingError
    """
    warnings.warn(
        'marshal_object will be deprecated in the next major release. '
        'Please use the more general entry-point offered in marshal_schema_object',
        DeprecationWarning,
    )
    null_decorator = _handle_null_value(
        swagger_spec=swagger_spec,
        object_schema=object_spec,
    )
    marshal_function = _marshaling_method_object(swagger_spec=swagger_spec, object_schema=object_spec)
    return null_decorator(marshal_function)(object_value)


def marshal_model(swagger_spec, model_spec, model_value):
    # type: (Spec, JSONDict, typing.Any) -> typing.Any
    """Marshal a Model instance into a json-like dict.

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :type model_spec: dict
    :type model_value: Model instance
    :rtype: dict
    :raises: SwaggerMappingError
    """
    warnings.warn(
        'marshal_model will be deprecated in the next major release. '
        'Please use the more general entry-point offered in marshal_schema_object',
        DeprecationWarning,
    )
    null_decorator = _handle_null_value(
        swagger_spec=swagger_spec,
        object_schema=model_spec,
    )
    marshal_function = _marshaling_method_object(swagger_spec=swagger_spec, object_schema=model_spec)
    return null_decorator(marshal_function)(model_value)


@_decorators.wrap_recursive_call_exception
@memoize_by_id
def _get_marshaling_method(swagger_spec, object_schema, required=False):
    # type: (Spec, JSONDict, bool) -> MarshalingMethod
    """
    Determine the method needed to marshal values of a defined object_schema
    The returned method will accept a single positional parameter that represent the value
    to be marshaled.

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :type object_schema: dict
    """
    object_schema = swagger_spec.deref(object_schema)
    null_decorator = _handle_null_value(
        swagger_spec=swagger_spec,
        object_schema=object_schema,
        is_nullable=not required,
    )
    object_type = get_type_from_schema(swagger_spec, object_schema)

    if object_type == 'array':
        return null_decorator(_marshaling_method_array(swagger_spec, object_schema))
    elif object_type == 'file':
        return null_decorator(_marshaling_method_file(swagger_spec, object_schema))
    elif object_type == 'object':
        return null_decorator(_marshaling_method_object(swagger_spec, object_schema))
    elif object_type in SWAGGER_PRIMITIVES:
        return null_decorator(_marshaling_method_primitive_type(swagger_spec, object_schema))
    elif object_type is None:
        return _no_op_marshaling
    else:
        return partial(
            _unknown_type_marshaling,
            object_type,
        )


def _no_op_marshaling(value):
    # type: (typing.Any) -> typing.Any
    return value


def _unknown_type_marshaling(object_type, value):
    # type: (typing.Union[typing.Type[dict], typing.Type[Model]], typing.Any) -> NoReturn
    raise SwaggerMappingError(
        'Unknown type {0} for value {1}'.format(
            object_type, value,
        ),
    )


def _raise_unknown_model(model_name, value):
    # type: (typing.Text, typing.Any) -> NoReturn
    raise SwaggerMappingError('Unknown model {0} when trying to marshal {1}'.format(model_name, value))


def _marshal_array(marshal_array_item_function, value):
    # type: (MarshalingMethod, typing.Any) -> typing.Any
    """
    Marshal a python list to its JSON list representation.

    :param marshal_array_item_function: Marshaling function for each array item
    :param value: Python list/tuple to marshal as JSON Array

    :raises: SwaggerMappingError
    """
    if not is_list_like(value):
        raise SwaggerMappingError('Expected list like type for {0}:{1}'.format(type(value), value))

    return [
        marshal_array_item_function(item)
        for item in value
    ]


def _marshaling_method_array(swagger_spec, object_schema):
    # type: (Spec, JSONDict) -> MarshalingMethod
    """
    Determine the marshaling method needed for a schema of a type array.

    The method will be responsible for the identification of the marshaling method of the array items.

    :param swagger_spec: Spec object
    :param object_schema: Schema of the object type
    """
    item_schema = swagger_spec.deref(swagger_spec.deref(object_schema).get('items', _NOT_FOUND))
    if item_schema is _NOT_FOUND:
        return _no_op_marshaling

    return partial(
        _marshal_array,
        _get_marshaling_method(swagger_spec=swagger_spec, object_schema=item_schema),
    )


def _marshaling_method_file(swagger_spec, object_schema):
    # type: (Spec, JSONDict) -> MarshalingMethod
    # TODO: Type file is not a valid type. It is present to support parameter marshaling (move to bravado_core.param)  # noqa: E501
    return _no_op_marshaling


def _marshal_object(
    swagger_spec,  # type: Spec
    properties_to_marshaling_function,  # type: typing.Dict[typing.Text, MarshalingMethod]
    additional_properties_marshaling_function,  # type: MarshalingMethod
    discriminator_property,  # type: typing.Optional[typing.Text]
    possible_discriminated_type_name_to_model,  # type: typing.Dict[typing.Text, Model]
    required_properties,  # type: typing.Set[typing.Text]
    nullable_properties,  # type: typing.Set[typing.Text]
    model_value,  # type: typing.Any
):
    # type: (...) -> typing.Any
    """
    Marshal a dict or Model instance into its JSON Object representation.

    :param swagger_spec: Spec object
    :param properties_to_marshaling_function: Mapping between property name and associated unmarshaling method
    :param additional_properties_marshaling_function: Unmarshaling function of eventual additional properties
    :param discriminator_property: Discriminator property name. It will be `None` if the schema is not a polymorphic schema
    :param possible_discriminated_type_name_to_model: Mapping of the possible dereferenced Model names and Model instances.
    :param required_properties: Set of required properties of the object schema
    :param nullable_properties: Set of nullable properties of the object schema
    :param model_value: Python dictionary or Model to marshal as JSON Object

    :raises: SwaggerMappingError
    """
    if not is_dict_like(model_value) and not isinstance(model_value, Model):
        raise SwaggerMappingError(
            "Expected type to be dict or Model to marshal value '{0}' to a dict. Was {1} instead.".format(
                model_value, type(model_value),
            ),
        )

    if discriminator_property:
        discriminator_value = model_value[discriminator_property]
        discriminated_model = possible_discriminated_type_name_to_model.get(discriminator_value)
        if discriminated_model is not None:
            marshaling_function = _get_marshaling_method(swagger_spec=swagger_spec, object_schema=discriminated_model._model_spec)
            return marshaling_function(model_value)

    marshaled_value = dict()
    for property_name in model_value:
        property_value = model_value[property_name]
        property_marshaling_function = properties_to_marshaling_function.get(
            property_name, additional_properties_marshaling_function,
        )
        if (
            property_value is None
            and property_name not in required_properties
            and property_name not in nullable_properties
            and property_name in properties_to_marshaling_function
        ):
            continue

        marshaled_value[property_name] = property_marshaling_function(property_value)

    return marshaled_value


def _marshaling_method_object(swagger_spec, object_schema):
    # type: (Spec, JSONDict) -> MarshalingMethod
    """
    Determine the marshaling method needed for a schema of a type object.

    The method will be responsible for the identification of:
     * required and nullable value for the all properties
     * marshaling methods of all the properties
     * marshaling method of the eventual additional properties
     * polymorphic nature of the object (`discriminator` attribute) and list of the associated models

    :param swagger_spec: Spec object
    :param object_schema: Schema of the object type
    """

    model_type = None  # type: typing.Optional[typing.Type[Model]]
    object_schema = swagger_spec.deref(object_schema)
    if MODEL_MARKER in object_schema:
        model_name = object_schema[MODEL_MARKER]
        model_type = swagger_spec.definitions.get(model_name)
        if model_type is None:
            return partial(
                _raise_unknown_model,
                model_name,
            )

    properties = collapsed_properties(object_schema, swagger_spec)
    required_properties = collapsed_required(object_schema, swagger_spec)
    properties_to_marshaling_function = {
        prop_name: _get_marshaling_method(
            swagger_spec=swagger_spec,
            object_schema=prop_schema,
            required=prop_name in required_properties,
        )
        for prop_name, prop_schema in iteritems(properties)
    }

    additional_properties_marshaling_function = _no_op_marshaling
    if object_schema.get('additionalProperties') is not False:
        additional_properties_schema = object_schema.get('additionalProperties', {})
        if additional_properties_schema not in ({}, True):
            additional_properties_marshaling_function = _get_marshaling_method(
                swagger_spec=swagger_spec,
                object_schema=additional_properties_schema,
            )

    discriminator_property = object_schema.get('discriminator')
    possible_discriminated_type_name_to_model = {}
    if model_type and object_schema.get('discriminator'):
        possible_discriminated_type_name_to_model.update({
            k: v
            for k, v in iteritems(swagger_spec.definitions)
            if model_type and model_type.__name__ in v._inherits_from
        })

    nullable_properties = {
        prop_name
        for prop_name, prop_schema in iteritems(properties)
        if schema.is_prop_nullable(swagger_spec, prop_schema)
    }

    return partial(
        _marshal_object,
        swagger_spec,
        properties_to_marshaling_function,
        additional_properties_marshaling_function,
        discriminator_property,
        possible_discriminated_type_name_to_model,
        required_properties,
        nullable_properties,
    )


def _marshal_primitive_type(primitive_type, swagger_format, value):
    # type: (typing.Text, SwaggerFormat, typing.Any) -> typing.Any
    """
    Marshal a python representation of a primitive type to its JSON representation.

    :param primitive_type: Primitive type of the schema to marshal
    :param swagger_format: Swagger format to apply during marshaling
    :param value: Python primitive value to marshal
    """
    try:
        return swagger_format.to_wire(value)
    except Exception as e:
        raise SwaggerMappingError(
            'Error while marshalling value={} to type={}/{}.'.format(
                value, primitive_type, swagger_format.format,
            ),
            e,
        )


def _marshaling_method_primitive_type(swagger_spec, object_schema):
    # type: (Spec, JSONDict) -> MarshalingMethod
    """
    Determine the marshaling method needed for a schema of a primitive type.

    The method will be responsible for the identification of the eventual :class:`bravado_core.formatter.SwaggerFormat`
    transformation to apply.

    :param swagger_spec: Spec object
    :param object_schema: Schema of the primitive type
    """
    format_name = schema.get_format(swagger_spec, object_schema)
    swagger_format = swagger_spec.get_format(format_name) if format_name is not None else None
    if swagger_format is not None:
        return partial(
            _marshal_primitive_type,
            get_type_from_schema(swagger_spec, object_schema),
            swagger_format,
        )
    else:
        return _no_op_marshaling
