# -*- coding: utf-8 -*-
import functools

import typing
from jsonschema import FormatChecker
from jsonschema import RefResolver
from jsonschema import validators
from jsonschema.exceptions import ValidationError
from jsonschema.validators import Draft4Validator
from swagger_spec_validator.ref_validators import in_scope

from bravado_core.model import MODEL_MARKER
from bravado_core.schema import is_param_spec
from bravado_core.schema import is_prop_nullable
from bravado_core.schema import is_required
from bravado_core.util import memoize_by_id


"""Draft4Validator is not completely compatible with Swagger 2.0 schema
objects like parameter, etc. Swagger20Validator is an extension of
Draft4Validator which customizes/wraps some of the operations of the default
validator.
"""

if getattr(typing, 'TYPE_CHECKING', False):
    from bravado_core._compat_typing import JSONDict
    from bravado_core.spec import Spec

    class ValidatorType(typing.Protocol):
        def __init__(
            self,
            spec_dict,  # type: JSONDict
            format_checker,  # type: FormatChecker
            resolver,  # type: RefResolver
        ):
            # type: (...) -> None
            pass

        def validate(self, obj):
            # type: (typing.Any) -> typing.Generator[ValidationError, None, None]
            pass


# Extract "original" validators as defined in jsonschema library. Saving those to constants to reduce indirections during validation.  # noqa
_DRAFT4_ENUM_VALIDATOR = Draft4Validator.VALIDATORS['enum']
_DRAFT4_FORMAT_VALIDATOR = Draft4Validator.VALIDATORS['format']
_DRAFT4_REQUIRED_VALIDATOR = Draft4Validator.VALIDATORS['required']
_DRAFT4_TYPE_VALIDATOR = Draft4Validator.VALIDATORS['type']


def format_validator(
    swagger_spec,  # type: Spec
    validator,  # type: Draft4Validator
    format,  # type: typing.Any
    instance,  # type: typing.Any
    schema,  # type: JSONDict
):
    # type: (...) -> typing.Generator[ValidationError, None, None]
    """Skip the `format` validator when a Swagger parameter value is None.
    Otherwise it will fail with a "Failed validating u'format' in schema" failure instead
    of letting the downstream `required_validator` do its job.
    Also skip when a Swagger property value is None and the schema contains
    the extension field `x-nullable` set to True.
    In all other cases, delegate to the existing Draft4 `format` validator.

    """
    if instance is None and (
        is_param_spec(swagger_spec, schema) or
        is_prop_nullable(swagger_spec, schema)
    ):
        return

    for error in _DRAFT4_FORMAT_VALIDATOR(validator, format, instance, schema):
        yield error


def type_validator(
    swagger_spec,  # type: Spec
    validator,  # type: Draft4Validator
    types,  # type: typing.Any
    instance,  # type: typing.Any
    schema,  # type: JSONDict
):
    # type: (...) -> typing.Generator[ValidationError, None, None]
    """Skip the `type` validator when a Swagger parameter value is None.
    Otherwise it will fail with a "None is not a valid type" failure instead
    of letting the downstream `required_validator` do its job.
    Also skip when a Swagger property value is None and the schema contains
    the extension field `x-nullable` set to True.
    In all other cases, delegate to the existing Draft4 `type` validator.

    :param swagger_spec: needed for access to deref()
    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :param validator: Validator class used to validate the object
    :type validator: :class:`Swagger20Validator` or
        :class:`jsonschema.validators.Draft4Validator`
    :param types: validate types
    :type types: string or list
    :param instance: object instance value
    :param schema: swagger spec for the object
    :type schema: dict
    """
    if instance is None and (
        is_param_spec(swagger_spec, schema) or
        is_prop_nullable(swagger_spec, schema)
    ):
        return

    for error in _DRAFT4_TYPE_VALIDATOR(validator, types, instance, schema):
        yield error


def required_validator(
    swagger_spec,  # type: Spec
    validator,  # type: Draft4Validator
    required,  # type: typing.Any
    instance,  # type: typing.Any
    schema,  # type: JSONDict
):
    # type: (...) -> typing.Generator[ValidationError, None, None]
    """Swagger 2.0 expects `required` to be a bool in the Parameter object,
    but a list of properties everywhere else.

    :param swagger_spec: needed for access to deref()
    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :param validator: Validator class used to validate the object
    :type validator: :class:`Swagger20Validator` or
        :class:`jsonschema.validators.Draft4Validator`
    :param required: value of `required` field
    :type required: boolean or list or None
    :param instance: object instance value
    :param schema: swagger spec for the object
    :type schema: dict
    """
    if is_param_spec(swagger_spec, schema):
        if required and instance is None:
            yield ValidationError('{0} is a required parameter.'.format(schema['name']))
    else:
        for error in _DRAFT4_REQUIRED_VALIDATOR(validator, required, instance, schema):
            yield error


def enum_validator(
    swagger_spec,  # type: Spec
    validator,  # type: Draft4Validator
    enums,  # type: typing.Any
    instance,  # type: typing.Any
    schema,  # type: JSONDict
):
    # type: (...) -> typing.Generator[ValidationError, None, None]
    """Swagger 2.0 allows enums to be validated against objects of type
    arrays, like query parameter (collectionFormat: multi)

    :param swagger_spec: needed for access to deref()
    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :param validator: Validator class used to validate the object
    :type validator: :class: `Swagger20Validator` or
        :class: `jsonschema.validators.Draft4Validator`
    :param enums: allowed enum values
    :type enums: list
    :param instance: enum instance value
    :param schema: swagger spec for the object
    :type schema: dict
    """

    if instance is None and is_prop_nullable(swagger_spec, schema):
        return

    if schema.get('type') == 'array':
        for element in instance:
            for error in _DRAFT4_ENUM_VALIDATOR(validator, enums, element, schema):
                yield error
        return

    # Handle optional enum params with no value
    if is_param_spec(swagger_spec, schema):
        if instance is None and not is_required(swagger_spec, schema):
            return

    for error in _DRAFT4_ENUM_VALIDATOR(validator, enums, instance, schema):
        yield error


def discriminator_validator(
    swagger_spec,  # type: Spec
    validator,  # type: Draft4Validator
    discriminator_attribute,  # type: typing.Any
    instance,  # type: typing.Any
    schema,  # type: JSONDict
):
    # type: (...) -> None
    """
    Validates instance against the schema defined by the discriminator attribute.

    [Swagger 2.0 Schema Object](http://swagger.io/specification/#schemaObject) allows discriminator field to be defined.
    discriminator field defines the attribute that will be used to discriminate the object type.

    NOTE: discriminator_validator assumes that discriminator_attribute is not None or empty

    :param swagger_spec: needed for access to deref()
    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :param validator: Validator class used to validate the object
    :type validator: :class: `Swagger20Validator` or
        :class: `jsonschema.validators.Draft4Validator`
    :param discriminator_attribute: name of the discriminator attribute
    :type discriminator_attribute: str
    :param instance: object instance value
    :type instance: dict
    :param schema: swagger spec for the object
    :type schema: dict
    """

    if instance is None and is_prop_nullable(swagger_spec, schema):
        return

    try:
        discriminator_value = instance[discriminator_attribute]
    except KeyError:
        raise ValidationError("'{}' is a required property".format(discriminator_attribute))

    if discriminator_value not in swagger_spec.definitions:
        raise ValidationError(
            message='\'{}\' is not a recognized schema'.format(discriminator_value),
        )

    if discriminator_value == schema[MODEL_MARKER]:
        return

    discriminated_schema = swagger_spec.definitions[discriminator_value]._model_spec
    if 'allOf' not in discriminated_schema:
        raise ValidationError(
            message='discriminated schema \'{}\' must inherit from \'{}\''.format(
                discriminator_value, schema[MODEL_MARKER],
            ),
        )

    schemas_to_remove = [s for s in discriminated_schema['allOf'] if swagger_spec.deref(s) == schema]
    if not schemas_to_remove:
        # Not checking against len(schemas_to_remove) > 1 because it should be prevented by swagger spec validation
        raise ValidationError(
            message='discriminated schema \'{}\' must inherit from \'{}\''.format(
                discriminator_value, schema[MODEL_MARKER],
            ),
        )

    # Remove the current schema from the allOf list in order to avoid unbounded recursion
    # (the current object is already validated against schema)
    # WARNING: This is especially important if internally_dereference_refs is set to true
    #   as we're modifying new_schema and new_schema is a dict (so mutable) we need to copy
    #   it in order to have a brand new dictionary that we can modify
    new_schema = discriminated_schema.copy()
    new_schema['allOf'] = [
        all_of_schema if all_of_schema not in schemas_to_remove else {}
        for all_of_schema in new_schema['allOf']
    ]

    from bravado_core.validate import validate_object  # Local import due to circular dependency
    validate_object(swagger_spec=swagger_spec, object_spec=new_schema, value=instance)


def ref_validator(
    validator,  # type: Draft4Validator
    ref,  # type: typing.Any
    instance,  # type: typing.Any
    schema,  # type: JSONDict
):
    # type: (...) -> typing.Generator[ValidationError, None, None]
    """When validating a request or response that contain $refs, jsonschema's
     RefResolver only contains scope (RefResolver._scopes_stack) for the
     request_spec or response_spec that it is fed. This scope doesn't contain
     the full scope built when ingesting the spec from its root
     (#/ in swagger.json). So, we need to modify the behavior of ref
     validation to use the `x-scope` annotations that were created during spec
     ingestion (see model_discovery in bravado_core/model.py).

    :param validator: Validator class used to validate the object
    :type validator: :class: `Swagger20Validator` or
        :class: `jsonschema.validators.Draft4Validator`
    :param ref: the target of the ref. eg. #/foo/bar/Baz
    :type ref: string
    :param instance: the object being validated
    :param schema: swagger spec that contains the ref.
        eg {'$ref': '#/foo/bar/Baz'}
    :type schema: dict
    """
    # This is a copy of jsonschema._validators.ref(..) with the
    # in_scope(..) context manager applied before any refs are resolved.
    resolve = getattr(validator.resolver, "resolve")
    if resolve is None:
        with in_scope(validator.resolver, schema):
            with validator.resolver.resolving(ref) as resolved:
                for error in validator.descend(instance, resolved):
                    yield error
    else:
        with in_scope(validator.resolver, schema):
            scope, resolved = validator.resolver.resolve(ref)
        validator.resolver.push_scope(scope)

        try:
            for error in validator.descend(instance, resolved):
                yield error
        finally:
            validator.resolver.pop_scope()


@memoize_by_id
def get_validator_type(swagger_spec):
    # type: (Spec) -> typing.Type[ValidatorType]
    """Create a custom jsonschema validator for Swagger 2.0 specs.

    :rtype: Its complicated. See jsonschema.validators.create()
    """
    return validators.extend(
        Draft4Validator,
        {
            '$ref': ref_validator,
            'required': functools.partial(required_validator, swagger_spec),
            'enum': functools.partial(enum_validator, swagger_spec),
            'type': functools.partial(type_validator, swagger_spec),
            'format': functools.partial(format_validator, swagger_spec),
            'discriminator': functools.partial(discriminator_validator, swagger_spec),
        },
    )
