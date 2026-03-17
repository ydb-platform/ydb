import itertools
import functools

import six

from flex._compat import Mapping, Sequence
from flex.exceptions import (
    ValidationError,
    ErrorList,
    ErrorDict,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    ARRAY,
    OBJECT,
)
from flex.decorators import skip_if_not_of_type
from flex.validation.reference import (
    LazyReferenceValidator,
)
from flex.validation.common import (
    noop,
    skip_if_empty,
    generate_type_validator,
    generate_format_validator,
    generate_multiple_of_validator,
    generate_minimum_validator,
    generate_maximum_validator,
    generate_min_length_validator,
    generate_max_length_validator,
    generate_min_items_validator,
    generate_max_items_validator,
    generate_unique_items_validator,
    generate_pattern_validator,
    generate_enum_validator,
    validate_object,
    generate_object_validator,
    generate_allof_validator,
    generate_anyof_validator,
)
from flex.datastructures import (
    ValidationDict,
)


@skip_if_empty
@skip_if_not_of_type(OBJECT)
def validate_required(value, required_fields, **kwargs):
    with ErrorDict() as errors:
        for key in required_fields:
            if key not in value:
                errors.add_error(key, MESSAGES['required']['required'])


def generate_required_validator(required, **kwargs):
    if required:
        return functools.partial(
            validate_required,
            required_fields=required,
        )
    else:
        return noop


@skip_if_empty
@skip_if_not_of_type(OBJECT)
def validate_min_properties(value, minimum, **kwargs):
    if len(value.keys()) < minimum:
        raise ValidationError(
            MESSAGES['min_properties']['invalid'].format(
                minimum, len(value.keys()),
            ),
        )


def generate_min_properties_validator(minProperties, **kwargs):
    return functools.partial(validate_min_properties, minimum=minProperties)


@skip_if_empty
@skip_if_not_of_type(OBJECT)
def validate_max_properties(value, maximum, **kwargs):
    if len(value.keys()) > maximum:
        raise ValidationError(
            MESSAGES['max_properties']['invalid'].format(
                maximum, len(value.keys()),
            ),
        )


def generate_max_properties_validator(maxProperties, **kwargs):
    return functools.partial(validate_max_properties, maximum=maxProperties)


def construct_items_validators(items, context):
    if isinstance(items, Mapping):
        items_validators = construct_schema_validators(
            schema=items,
            context=context,
        )
    elif isinstance(items, six.string_types):
        items_validators = {
            '$ref': SchemaReferenceValidator(items, context),
        }
    else:
        assert 'Should not be possible'
    return items_validators


@skip_if_not_of_type(ARRAY)
@skip_if_empty
def validate_items(objs, field_validators, **kwargs):
    errors = ErrorList()
    for obj, _field_validators in zip(objs, field_validators):
        try:
            validate_object(
                obj,
                field_validators=_field_validators,
                **kwargs
            )
        except ValidationError as e:
            errors.add_error(e.detail)

    if errors:
        raise ValidationError(errors)


def generate_items_validator(items, context, **kwargs):
    if isinstance(items, Mapping):
        # If items is a reference or a schema, we pass it through as an
        # ever repeating list of the same validation dictionary, thus
        # validating all of the objects against the same schema.
        items_validators = itertools.repeat(construct_items_validators(
            items,
            context,
        ))
    elif isinstance(items, Sequence):
        # We generate a list of validator dictionaries and then chain it
        # with an empty schema that repeats forever.  This ensures that if
        # the array of objects to be validated is longer than the array of
        # validators, then the extra elements will always validate since
        # they will be validated against an empty schema.
        items_validators = itertools.chain(
            map(functools.partial(construct_items_validators, context=context), items),
            itertools.repeat({}),
        )
    else:
        assert "Should not be possible"
    return functools.partial(
        validate_items, field_validators=items_validators,
    )


@skip_if_not_of_type(OBJECT)
@skip_if_empty
def validate_additional_properties(obj, additional_properties, properties, **kwargs):
    if additional_properties is False:
        allowed_properties = set(properties.keys())
        actual_properties = set(obj.keys())
        extra_properties = actual_properties.difference(allowed_properties)
        if extra_properties:
            raise ValidationError(
                MESSAGES['additional_properties']['extra_properties'].format(
                    repr(extra_properties),
                )
            )


def generate_additional_properties_validator(additionalProperties, properties, **kwargs):
    return functools.partial(
        validate_additional_properties,
        additional_properties=additionalProperties,
        properties=properties,
    )


validator_mapping = {
    'type': generate_type_validator,
    'multipleOf': generate_multiple_of_validator,
    'minimum': generate_minimum_validator,
    'maximum': generate_maximum_validator,
    'minLength': generate_min_length_validator,
    'maxLength': generate_max_length_validator,
    'minItems': generate_min_items_validator,
    'maxItems': generate_max_items_validator,
    'uniqueItems': generate_unique_items_validator,
    'enum': generate_enum_validator,
    'minProperties': generate_min_properties_validator,
    'maxProperties': generate_max_properties_validator,
    'pattern': generate_pattern_validator,
    'format': generate_format_validator,
    'required': generate_required_validator,
    'items': generate_items_validator,
    'allOf': generate_allof_validator,
    'anyOf': generate_anyof_validator,
}


def construct_schema_validators(schema, context):
    """
    Given a schema object, construct a dictionary of validators needed to
    validate a response matching the given schema.

    Special Cases:
        - $ref:
            These validators need to be Lazily evaluating so that circular
            validation dependencies do not result in an infinitely deep
            validation chain.
        - properties:
            These validators are meant to apply to properties of the object
            being validated rather than the object itself.  In this case, we
            need recurse back into this function to generate a dictionary of
            validators for the property.
    """
    validators = ValidationDict()
    if '$ref' in schema:
        validators.add_validator(
            '$ref', SchemaReferenceValidator(schema['$ref'], context),
        )
    if 'properties' in schema:
        for property_, property_schema in schema['properties'].items():
            property_validator = generate_object_validator(
                schema=property_schema,
                context=context,
            )
            validators.add_property_validator(property_, property_validator)
    if schema.get('additionalProperties') is False:
        validators.add_validator(
            'additionalProperties',
            generate_additional_properties_validator(context=context, **schema),
        )
    assert 'context' not in schema
    for key in schema:
        if key in validator_mapping:
            validators.add_validator(key, validator_mapping[key](context=context, **schema))
    return validators


class SchemaReferenceValidator(LazyReferenceValidator):
    """
    This class acts as a lazy validator for references in schemas to prevent an
    infinite recursion error when a schema references itself, or there is a
    reference loop between more than one schema.

    The validator is only constructed if validator is needed.
    """
    validators_constructor = construct_schema_validators
