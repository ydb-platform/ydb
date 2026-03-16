# Standard libraries
import re

from flex.datastructures import ValidationDict
from flex.utils import is_non_string_iterable
from flex.exceptions import (
    ValidationError,
    MultipleParametersFound,
    NoParameterFound,
)
from flex.error_messages import MESSAGES
from flex.context_managers import ErrorDict
from flex.validation.reference import (
    LazyReferenceValidator,
)
from flex.validation.common import (
    noop,
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
    generate_object_validator,
    generate_value_processor,
)
from flex.validation.schema import (
    construct_schema_validators,
    generate_items_validator,
)
from flex.parameters import find_parameter
from flex.paths import NORMALIZE_SLASH_REGEX, path_to_regex
from flex.constants import EMPTY


def validate_required(value, **kwargs):
    if value is EMPTY:
        raise ValidationError(MESSAGES['required']['required'])


def generate_required_validator(required, **kwargs):
    if required:
        return validate_required
    else:
        return noop


def type_cast_parameters(parameter_values, parameter_definitions, context):
    typed_parameters = {}
    for key in parameter_values.keys():
        try:
            parameter_definition = find_parameter(parameter_definitions, name=key)
        except (KeyError, MultipleParametersFound, NoParameterFound):
            continue
        value = parameter_values[key]
        value_processor = generate_value_processor(context=context, **parameter_definition)
        typed_parameters[key] = value_processor(value)
    return typed_parameters


def get_path_parameter_values(target_path, api_path, path_parameters, context):
    raw_values = path_to_regex(
        api_path,
        path_parameters,
    ).match(target_path).groupdict()
    return type_cast_parameters(raw_values, path_parameters, context=context)


def validate_path_parameters(target_path, api_path, path_parameters, context):
    """
    Helper function for validating a request path
    """
    base_path = context.get('basePath', '')
    full_api_path = re.sub(NORMALIZE_SLASH_REGEX, '/', base_path + api_path)
    parameter_values = get_path_parameter_values(
        target_path, full_api_path, path_parameters, context,
    )
    validate_parameters(parameter_values, path_parameters, context=context)


def validate_query_parameters(raw_query_data, query_parameters, context):
    query_data = {}
    for key, value in raw_query_data.items():
        if is_non_string_iterable(value) and len(value) == 1:
            query_data[key] = value[0]
        else:
            query_data[key] = value
    query_data = type_cast_parameters(query_data, query_parameters, context)
    validate_parameters(query_data, query_parameters, context)


def validate_parameters(parameter_values, parameters, context):
    validators = construct_multi_parameter_validators(parameters, context=context)

    with ErrorDict() as errors:
        for key, validator in validators.items():
            try:
                validator(parameter_values.get(key, EMPTY))
            except ValidationError as err:
                errors[key].add_error(err.detail)


def construct_parameter_validators(parameter, context):
    """
    Constructs a dictionary of validator functions for the provided parameter
    definition.
    """
    validators = ValidationDict()
    if '$ref' in parameter:
        validators.add_validator(
            '$ref', ParameterReferenceValidator(parameter['$ref'], context),
        )
    for key in parameter:
        if key in validator_mapping:
            validators.add_validator(
                key,
                validator_mapping[key](context=context, **parameter),
            )
    if 'schema' in parameter:
        schema_validators = construct_schema_validators(parameter['schema'], context=context)
        for key, value in schema_validators.items():
            validators.setdefault(key, value)
    return validators


validator_mapping = {
    'type': generate_type_validator,
    'format': generate_format_validator,
    'required': generate_required_validator,
    'multipleOf': generate_multiple_of_validator,
    'minimum': generate_minimum_validator,
    'maximum': generate_maximum_validator,
    'minLength': generate_min_length_validator,
    'maxLength': generate_max_length_validator,
    'minItems': generate_min_items_validator,
    'maxItems': generate_max_items_validator,
    'uniqueItems': generate_unique_items_validator,
    'enum': generate_enum_validator,
    'pattern': generate_pattern_validator,
    'items': generate_items_validator,
}


def construct_multi_parameter_validators(parameters, context):
    """
    Given an iterable of parameters, returns a dictionary of validator
    functions for each parameter.  Note that this expects the parameters to be
    unique in their name value, and throws an error if this is not the case.
    """
    validators = ValidationDict()
    for parameter in parameters:
        key = parameter['name']
        if key in validators:
            raise ValueError("Duplicate parameter name {0}".format(key))
        parameter_validators = construct_parameter_validators(parameter, context=context)
        validators.add_validator(
            key,
            generate_object_validator(field_validators=parameter_validators),
        )

    return validators


class ParameterReferenceValidator(LazyReferenceValidator):
    validators_constructor = construct_parameter_validators
