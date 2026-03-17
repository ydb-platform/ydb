from __future__ import unicode_literals
import re
import decimal
import operator
import functools
import collections
import itertools
import json

import os
import six


from flex._compat import Mapping, Sequence
from flex.exceptions import (
    ValidationError,
    ErrorDict,
    ErrorList,
    MultiplePathsFound,
)
from flex.datastructures import (
    ValidationDict,
    ValidationList,
)
from flex.formats import registry
from flex.utils import (
    is_value_of_any_type,
    is_non_string_iterable,
    get_type_for_value,
    cast_value_to_type,
    deep_equal,
)
from flex.functional import chain_reduce_partial
from flex.paths import (
    match_path_to_api_path,
)
from flex.constants import (
    NULL,
    NUMBER,
    STRING,
    ARRAY,
    MULTI,
    OBJECT,
    DELIMETERS,
    REQUEST_METHODS,
    FLEX_DISABLE_X_NULLABLE,
)
from flex.decorators import (
    skip_if_not_of_type,
    skip_if_empty,
    suffix_reserved_words,
)
from flex.error_messages import MESSAGES


@skip_if_empty
def validate_type(value, types, **kwargs):
    """
    Validate that the value is one of the provided primative types.
    """
    if not is_value_of_any_type(value, types):
        raise ValidationError(MESSAGES['type']['invalid'].format(
            repr(value), get_type_for_value(value), types,
        ))


@suffix_reserved_words
def generate_type_validator(type_, **kwargs):
    """
    Generates a callable validator for the given type or iterable of types.
    """
    if is_non_string_iterable(type_):
        types = tuple(type_)
    else:
        types = (type_,)
    # support x-nullable since Swagger 2.0 doesn't support null type
    # (see https://github.com/OAI/OpenAPI-Specification/issues/229)
    if kwargs.get('x-nullable', False) and NULL not in types:
        types = types + (NULL,)
    return functools.partial(validate_type, types=types)


@suffix_reserved_words
def generate_format_validator(format_, **kwargs):
    """
    Returns the validator function for the given format, as registered with the
    format registry.
    """
    if format_ in registry:
        return registry[format_]
    else:
        return noop


def noop(*args, **kwargs):
    """
    No-Op validator that does nothing.
    """
    pass


@skip_if_empty
@skip_if_not_of_type(NUMBER)
def validate_multiple_of(value, divisor, **kwargs):
    """
    Given a value and a divisor, validate that the value is divisible by the
    divisor.
    """
    if not decimal.Decimal(str(value)) % decimal.Decimal(str(divisor)) == 0:
        raise ValidationError(
            MESSAGES['multiple_of']['invalid'].format(divisor, value),
        )


def generate_multiple_of_validator(multipleOf, **kwargs):
    return functools.partial(validate_multiple_of, divisor=multipleOf)


@skip_if_empty
@skip_if_not_of_type(NUMBER)
def validate_minimum(value, minimum, is_exclusive, **kwargs):
    """
    Validator function for validating that a value does not violate it's
    minimum allowed value.  This validation can be inclusive, or exclusive of
    the minimum depending on the value of `is_exclusive`.
    """
    if is_exclusive:
        comparison_text = "greater than"
        compare_fn = operator.gt
    else:
        comparison_text = "greater than or equal to"
        compare_fn = operator.ge

    if not compare_fn(value, minimum):
        raise ValidationError(
            MESSAGES['minimum']['invalid'].format(value, comparison_text, minimum),
        )


def generate_minimum_validator(minimum, exclusiveMinimum=False, **kwargs):
    """
    Generator function returning a callable for minimum value validation.
    """
    return functools.partial(validate_minimum, minimum=minimum, is_exclusive=exclusiveMinimum)


@skip_if_empty
@skip_if_not_of_type(NUMBER)
def validate_maximum(value, maximum, is_exclusive, **kwargs):
    """
    Validator function for validating that a value does not violate it's
    maximum allowed value.  This validation can be inclusive, or exclusive of
    the maximum depending on the value of `is_exclusive`.
    """
    if is_exclusive:
        comparison_text = "less than"
        compare_fn = operator.lt
    else:
        comparison_text = "less than or equal to"
        compare_fn = operator.le

    if not compare_fn(value, maximum):
        raise ValidationError(
            MESSAGES['maximum']['invalid'].format(value, comparison_text, maximum),
        )


def generate_maximum_validator(maximum, exclusiveMaximum=False, **kwargs):
    """
    Generator function returning a callable for maximum value validation.
    """
    return functools.partial(validate_maximum, maximum=maximum, is_exclusive=exclusiveMaximum)


@skip_if_empty
@skip_if_not_of_type(STRING)
def validate_min_length(value, minLength, **kwargs):
    if len(value) < minLength:
        raise ValidationError(MESSAGES['min_length']['invalid'].format(minLength))


def generate_min_length_validator(minLength, **kwargs):
    """
    Generates a validator for enforcing the minLength of a string.
    """
    return functools.partial(validate_min_length, minLength=minLength)


@skip_if_empty
@skip_if_not_of_type(STRING)
def validate_max_length(value, maxLength, **kwargs):
    if len(value) > maxLength:
        raise ValidationError(MESSAGES['max_length']['invalid'].format(maxLength))


def generate_max_length_validator(maxLength, **kwargs):
    """
    Generates a validator for enforcing the maxLength of a string.
    """
    return functools.partial(validate_max_length, maxLength=maxLength)


@skip_if_empty
@skip_if_not_of_type(ARRAY)
def validate_min_items(value, minimum, **kwargs):
    """
    Validator for ARRAY types to enforce a minimum number of items allowed for
    the ARRAY to be valid.
    """
    if len(value) < minimum:
        raise ValidationError(
            MESSAGES['min_items']['invalid'].format(
                minimum, len(value),
            ),
        )


def generate_min_items_validator(minItems, **kwargs):
    """
    Generator function returning a callable for minItems validation.
    """
    return functools.partial(validate_min_items, minimum=minItems)


@skip_if_empty
@skip_if_not_of_type(ARRAY)
def validate_max_items(value, maximum, **kwargs):
    """
    Validator for ARRAY types to enforce a maximum number of items allowed for
    the ARRAY to be valid.
    """
    if len(value) > maximum:
        raise ValidationError(
            MESSAGES['max_items']['invalid'].format(
                maximum, len(value),
            ),
        )


def generate_max_items_validator(maxItems, **kwargs):
    """
    Generator function returning a callable for maxItems validation.
    """
    return functools.partial(validate_max_items, maximum=maxItems)


@skip_if_empty
@skip_if_not_of_type(ARRAY)
def validate_unique_items(value, **kwargs):
    """
    Validator for ARRAY types to enforce that all array items must be unique.
    """
    # we can't just look at the items themselves since 0 and False are treated
    # the same as dictionary keys, and objects aren't hashable.

    counter = collections.Counter((
        json.dumps(v, sort_keys=True) for v in value
    ))
    dupes = [json.loads(v) for v, count in counter.items() if count > 1]
    if dupes:
        raise ValidationError(
            MESSAGES['unique_items']['invalid'].format(
                repr(dupes),
            ),
        )


def generate_unique_items_validator(uniqueItems, **kwargs):
    """
    Returns the unique_item_validator if uniqueItems is set to True, otherwise
    it returns the noop function.
    """
    if uniqueItems:
        return validate_unique_items
    else:
        return noop


@skip_if_empty
@skip_if_not_of_type(STRING)
def validate_pattern(value, regex, **kwargs):
    if not regex.match(value):
        raise ValidationError(
            MESSAGES['pattern']['invalid'].format(value, regex.pattern),
        )


def generate_pattern_validator(pattern, **kwargs):
    return functools.partial(validate_pattern, regex=re.compile(pattern))


@skip_if_empty
def validate_enum(value, options, **kwargs):
    if not any(deep_equal(value, option) for option in options):
        raise ValidationError(
            MESSAGES['enum']['invalid'].format(
                value, options,
            )
        )


def generate_enum_validator(enum, **kwargs):
    x_nullable_enabled = all((
        FLEX_DISABLE_X_NULLABLE not in os.environ,
        kwargs.get('x-nullable') is True,
        None not in enum
    ))
    if x_nullable_enabled:
        enum_values = tuple(itertools.chain(enum, [None]))
    else:
        enum_values = enum
    return functools.partial(validate_enum, options=enum_values)


@skip_if_empty
def validate_allof_anyof(value, sub_schemas, context, method, **kwargs):
    from flex.validation.schema import (
        construct_schema_validators,
    )

    messages = []
    success = []
    for schema in sub_schemas:
        schema_validators = construct_schema_validators(schema, context)
        try:
            schema_validators.validate_object(value, context=context)
        except ValidationError as err:
            messages.append(err.messages)
            success.append(False)
        else:
            success.append(True)

    if not method(success):
        raise ValidationError(messages)

    return value


def generate_allof_validator(allOf, context, **kwargs):
    return functools.partial(validate_allof_anyof, sub_schemas=allOf, context=context, method=all)


def generate_anyof_validator(anyOf, context, **kwargs):
    return functools.partial(validate_allof_anyof, sub_schemas=anyOf, context=context, method=any)


def add_polymorphism_requirements(obj, schema, context, schema_validators):
    try:
        object_type = obj[schema['discriminator']]
    except KeyError:
        raise ValidationError("No discriminator found on instance [{0}].".format(obj))

    try:
        object_schema = context['definitions'][object_type]
    except KeyError:
        raise ValidationError("No definition for class [{0}]".format(object_type))

    from flex.validation.schema import (
        construct_schema_validators,
    )
    subtype_validators = construct_schema_validators(object_schema, context)
    schema_validators.update(subtype_validators)
    return schema_validators


def validate_object(obj, field_validators=None, non_field_validators=None,
                    schema=None, context=None):
    """
    Takes a mapping and applies a mapping of validator functions to it
    collecting and reraising any validation errors that occur.
    """
    if schema is None:
        schema = {}
    if context is None:
        context = {}
    if field_validators is None:
        field_validators = ValidationDict()
    if non_field_validators is None:
        non_field_validators = ValidationList()

    from flex.validation.schema import (
        construct_schema_validators,
    )
    schema_validators = construct_schema_validators(schema, context)
    if '$ref' in schema_validators and hasattr(schema_validators['$ref'], 'validators'):
        ref_ = field_validators.pop('$ref')
        for k, v in ref_.validators.items():
            if k not in schema_validators:
                schema_validators.add_validator(k, v)

    if 'discriminator' in schema:
        schema_validators = add_polymorphism_requirements(obj, schema, context, schema_validators)
        # delete resolved discriminator to avoid infinite recursion
        del schema['discriminator']

    schema_validators.update(field_validators)
    schema_validators.validate_object(obj, context=context)
    non_field_validators.validate_object(obj, context=context)
    return obj


def generate_object_validator(**kwargs):
    return functools.partial(validate_object, **kwargs)


@skip_if_empty
@skip_if_not_of_type(OBJECT)
def apply_validator_to_object(obj, validator, **kwargs):
    with ErrorDict() as errors:
        for key, value in obj.items():
            try:
                validator(value, **kwargs)
            except ValidationError as err:
                errors.add_error(key, err.detail)


@skip_if_empty
@skip_if_not_of_type(ARRAY)
def apply_validator_to_array(values, validator, **kwargs):
    with ErrorList() as errors:
        for value in values:
            try:
                validator(value, **kwargs)
            except ValidationError as err:
                errors.add_error(err.detail)


def add_string_into_list(raw_query_data):
    if isinstance(raw_query_data, six.string_types):
        return [raw_query_data]
    return raw_query_data


@suffix_reserved_words
def generate_value_processor(type_, collectionFormat=None, items=None, **kwargs):
    """
    Create a callable that will take the string value of a header and cast it
    to the appropriate type.  This can involve:

    - splitting a header of type 'array' by its delimeters.
    - type casting the internal elements of the array.
    """
    processors = []
    if is_non_string_iterable(type_):
        assert False, "This should not be possible"
    else:
        if type_ == ARRAY and collectionFormat:
            if collectionFormat in DELIMETERS:
                delimeter = DELIMETERS[collectionFormat]
                # split the string based on the delimeter specified by the
                # `collectionFormat`
                processors.append(operator.methodcaller('split', delimeter))
            else:
                if collectionFormat != MULTI:
                    raise TypeError("collectionFormat not implemented")
                processors.append(add_string_into_list)
            # remove any Falsy values like empty strings.
            processors.append(functools.partial(filter, bool))
            # strip off any whitespace
            processors.append(functools.partial(map, operator.methodcaller('strip')))
            if items is not None:
                if isinstance(items, Mapping):
                    items_processors = itertools.repeat(
                        generate_value_processor(**items)
                    )
                elif isinstance(items, Sequence):
                    items_processors = itertools.chain(
                        (generate_value_processor(**item) for item in items),
                        itertools.repeat(lambda v: v),
                    )
                elif isinstance(items, six.string_types):
                    raise NotImplementedError("Not implemented")
                else:
                    assert False, "Should not be possible"
                # 1. zip the processor and the array items together
                # 2. apply the processor to each array item.
                # 3. cast the starmap generator to a list.
                processors.append(
                    chain_reduce_partial(
                        functools.partial(zip, items_processors),
                        functools.partial(itertools.starmap, lambda fn, v: fn(v)),
                        list,
                    )
                )
        else:
            processors.append(
                functools.partial(cast_value_to_type, type_=type_)
            )

    def processor(value, **kwargs):
        try:
            return chain_reduce_partial(*processors)(value)
        except (ValueError, TypeError):
            return value

    return processor


def validate_request_method_to_operation(request_method, path_definition):
    """
    Given a request method, validate that the request method is valid for the
    api path.

    If so, return the operation definition related to this request method.
    """
    try:
        operation_definition = path_definition[request_method]
    except KeyError:
        allowed_methods = set(REQUEST_METHODS).intersection(path_definition.keys())
        raise ValidationError(
            MESSAGES['request']['invalid_method'].format(
                request_method, allowed_methods,
            ),
        )
    return operation_definition


def validate_path_to_api_path(path, paths, basePath='', context=None, **kwargs):
    """
    Given a path, find the api_path it matches.
    """
    if context is None:
        context = {}
    try:
        api_path = match_path_to_api_path(
            path_definitions=paths,
            target_path=path,
            base_path=basePath,
            context=context,
        )
    except LookupError as err:
        raise ValidationError(str(err))
    except MultiplePathsFound as err:
        raise ValidationError(str(err))

    return api_path


def validate_content_type(content_type, content_types, **kwargs):
    if content_type:
        # only check MIME type, ignore parameters
        content_type, _, _ = content_type.partition(';')
        if content_type not in content_types:
            raise ValidationError(
                MESSAGES['content_type']['invalid'].format(
                    content_type, content_types,
                ),
            )
