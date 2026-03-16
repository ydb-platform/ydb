from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    STRING,
    PARAMETER_IN_VALUES,
    PATH,
    BODY,
    EMPTY,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    pull_keys_from_obj,
    suffix_reserved_words,
    skip_if_any_kwargs_empty,
)


in_schema = {
    'type': STRING,
    'enum': PARAMETER_IN_VALUES,
}


@pull_keys_from_obj('in', 'required')
@skip_if_any_kwargs_empty('in')
@suffix_reserved_words
def validate_path_parameters_must_be_required(in_, required, **kwargs):
    if in_ == PATH:
        if required is not True:
            raise ValidationError(MESSAGES['required']['path_parameters_must_be_required'])


@pull_keys_from_obj('in', 'schema')
@skip_if_any_kwargs_empty('in')
@suffix_reserved_words
def validate_body_parameters_must_include_a_schema(in_, schema, **kwargs):
    if in_ == BODY:
        if schema is EMPTY:
            raise ValidationError(MESSAGES['schema']['body_parameters_must_include_a_schema'])


@pull_keys_from_obj('in', 'type')
@skip_if_any_kwargs_empty('in')
@suffix_reserved_words
def validate_type_declared_for_non_body_parameters(in_, type_, **kwargs):
    if in_ != BODY:
        if type_ is EMPTY:
            raise ValidationError(MESSAGES['type']['non_body_parameters_must_declare_a_type'])


in_validator = generate_object_validator(
    schema=in_schema,
)
