import functools

from flex.datastructures import ValidationDict
from flex.exceptions import ValidationError
from flex.functional import chain_reduce_partial
from flex.functional import attrgetter
from flex.context_managers import ErrorDict
from flex.http import (
    Request,
)
from flex.constants import (
    QUERY,
    PATH,
    HEADER,
    BODY,
)
from flex.parameters import (
    filter_parameters,
    merge_parameter_lists,
    dereference_parameter_list,
)
from flex.validation.parameter import (
    validate_query_parameters,
    construct_parameter_validators,
)
from flex.validation.header import (
    construct_header_validators,
)
from flex.validation.path import (
    generate_path_parameters_validator,
)
from flex.validation.common import (
    noop,
    generate_value_processor,
    generate_object_validator,
    validate_content_type,
)


def validate_operation(request, validators, **kwargs):
    with ErrorDict() as errors:
        for key, validator in validators.items():
            try:
                validator(request, **kwargs)
            except ValidationError as err:
                errors[key].add_error(err.detail)


def validate_request_content_type(request, content_types, **kwargs):
    assert isinstance(request, Request)
    validate_content_type(request.content_type, content_types, **kwargs)


def generate_request_content_type_validator(consumes, **kwargs):
    validator = functools.partial(
        validate_request_content_type,
        content_types=consumes,
    )
    return validator


def generate_header_validator(headers, context, **kwargs):
    """
    Generates a validation function that will validate a dictionary of headers.
    """
    validators = ValidationDict()
    for header_definition in headers:
        header_processor = generate_value_processor(
            context=context,
            **header_definition
        )
        header_validator = generate_object_validator(
            field_validators=construct_header_validators(header_definition, context=context),
        )
        validators.add_property_validator(
            header_definition['name'],
            chain_reduce_partial(
                header_processor,
                header_validator,
            ),
        )
    return generate_object_validator(field_validators=validators)


def generate_form_data_validator(form_data_parameters, context, **kwargs):
    pass


def generate_request_body_validator(body_parameters, context, **kwargs):
    if len(body_parameters) > 1:
        raise ValueError("Too many body parameters.  Should only be one")
    elif not body_parameters:
        return noop
    body_validators = construct_parameter_validators(
        body_parameters[0], context=context,
    )
    return generate_object_validator(field_validators=body_validators)


def generate_parameters_validator(api_path, path_definition, parameters,
                                  context, **kwargs):
    """
    Generates a validator function to validate.

    - request.path against the path parameters.
    - request.query against the query parameters.
    - request.headers against the header parameters.
    - TODO: request.body against the body parameters.
    - TODO: request.formData against any form data.
    """
    # TODO: figure out how to merge this with the same code in response
    # validation.
    validators = ValidationDict()
    path_level_parameters = dereference_parameter_list(
        path_definition.get('parameters', []),
        context,
    )
    operation_level_parameters = dereference_parameter_list(
        parameters,
        context,
    )

    all_parameters = merge_parameter_lists(
        path_level_parameters,
        operation_level_parameters,
    )

    # PATH
    in_path_parameters = filter_parameters(all_parameters, in_=PATH)
    validators.add_validator(
        'path',
        chain_reduce_partial(
            attrgetter('path'),
            generate_path_parameters_validator(api_path, in_path_parameters, context),
        ),
    )

    # QUERY
    in_query_parameters = filter_parameters(all_parameters, in_=QUERY)
    validators.add_validator(
        'query',
        chain_reduce_partial(
            attrgetter('query_data'),
            functools.partial(
                validate_query_parameters,
                query_parameters=in_query_parameters,
                context=context,
            ),
        ),
    )

    # HEADERS
    in_header_parameters = filter_parameters(all_parameters, in_=HEADER)
    validators.add_validator(
        'headers',
        chain_reduce_partial(
            attrgetter('headers'),
            generate_header_validator(in_header_parameters, context),
        ),
    )

    # FORM_DATA
    # in_form_data_parameters = filter_parameters(all_parameters, in_=FORM_DATA)
    # validators.add_validator(
    #     'form_data',
    #     chain_reduce_partial(
    #         attrgetter('data'),
    #         generate_form_data_validator(in_form_data_parameters, context),
    #     )
    # )

    # REQUEST_BODY
    in_request_body_parameters = filter_parameters(all_parameters, in_=BODY)
    validators.add_validator(
        'request_body',
        chain_reduce_partial(
            attrgetter('data'),
            generate_request_body_validator(in_request_body_parameters, context),
        )
    )

    return generate_object_validator(field_validators=validators)


validator_mapping = {
    'consumes': generate_request_content_type_validator,
    'parameters': generate_parameters_validator,
    'headers': generate_header_validator,
}


def construct_operation_validators(api_path, path_definition, operation_definition, context):
    """
    - consumes (did the request conform to the content types this api consumes)
    - produces (did the response conform to the content types this endpoint produces)
    - parameters (did the parameters of this request validate)
      TODO: move path parameter validation to here, because each operation
            can override any of the path level parameters.
    - schemes (was the request scheme correct)
    - security: TODO since security isn't yet implemented.
    """
    validators = {}

    # sanity check
    assert 'context' not in operation_definition
    assert 'api_path' not in operation_definition
    assert 'path_definition' not in operation_definition

    for key in operation_definition.keys():
        if key not in validator_mapping:
            # TODO: is this the right thing to do?
            continue
        validators[key] = validator_mapping[key](
            context=context,
            api_path=api_path,
            path_definition=path_definition,
            **operation_definition
        )

    # Global defaults
    if 'consumes' in context and 'consumes' not in validators:
        validators['consumes'] = validator_mapping['consumes'](**context)
    if 'parameters' in path_definition and 'parameters' not in validators:
        validators['parameters'] = validator_mapping['parameters'](
            context=context,
            api_path=api_path,
            path_definition=path_definition,
            parameters=path_definition['parameters'],
            **operation_definition
        )

    return validators
