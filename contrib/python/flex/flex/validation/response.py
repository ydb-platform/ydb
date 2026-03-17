import functools

from flex.datastructures import ValidationDict
from flex.exceptions import ValidationError
from flex.functional import chain_reduce_partial
from flex.functional import attrgetter, methodcaller
from flex.context_managers import ErrorDict
from flex.validation.common import (
    validate_object,
    validate_path_to_api_path,
    validate_request_method_to_operation,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    EMPTY,
    PATH,
)
from flex.parameters import (
    filter_parameters,
    merge_parameter_lists,
    dereference_parameter_list,
)
from flex.validation.header import (
    construct_header_validators,
)
from flex.validation.path import (
    generate_path_parameters_validator,
)
from flex.validation.common import (
    generate_value_processor,
    validate_content_type,
)
from flex.http import Response
from flex.utils import dereference_reference


def validate_status_code_to_response_definition(response, operation_definition):
    """
    Given a response, validate that the response status code is in the accepted
    status codes defined by this endpoint.

    If so, return the response definition that corresponds to the status code.
    """
    status_code = response.status_code
    operation_responses = {str(code): val for code, val
                           in operation_definition['responses'].items()}

    key = status_code
    if key not in operation_responses:
        key = 'default'

    try:
        response_definition = operation_responses[key]
    except KeyError:
        raise ValidationError(
            MESSAGES['response']['invalid_status_code'].format(
                status_code, ', '.join(operation_responses.keys()),
            ),
        )
    return response_definition


def generate_response_body_validator(schema, context, **kwargs):
    return chain_reduce_partial(
        attrgetter('data'),
        functools.partial(
            validate_object,
            schema=schema,
            context=context,
        ),
    )


def generate_response_header_validator(headers, context, **kwargs):
    validators = ValidationDict()
    for key, header_definition in headers.items():
        # generate a function that will attempt to cast the header to the
        # appropriate type.
        header_processor = generate_value_processor(
            context=context,
            **header_definition
        )
        # generate a function that will validate the header.
        header_validator = functools.partial(
            validate_object,
            field_validators=construct_header_validators(header_definition, context=context),
        )
        # Chain the type casting function, the individual header validation
        # function with a methodcaller that will fetch the header with
        # `response.headers.get(header_name, EMPTY)` and then feed that into
        # the type casting function and then into the validation function.
        validators.add_validator(key, chain_reduce_partial(
            methodcaller('get', key, EMPTY),
            header_processor,
            header_validator,
        ))
    return chain_reduce_partial(
        attrgetter('headers'),
        functools.partial(validate_object, field_validators=validators),
    )


def validate_response_content_type(response, content_types, **kwargs):
    assert isinstance(response, Response)  # TODO: remove this sanity check
    validate_content_type(response.content_type, content_types)


def generate_response_content_type_validator(produces, **kwargs):
    return functools.partial(
        validate_response_content_type,
        content_types=produces,
    )


def generate_path_validator(api_path, path_definition, parameters,
                            context, **kwargs):
    """
    Generates a callable for validating the parameters in a response object.
    """
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
    return chain_reduce_partial(
        attrgetter('path'),
        generate_path_parameters_validator(api_path, in_path_parameters, context),
    )


validator_mapping = {
    # TODO: tests for parameters AND figure out how to do parameters.
    'headers': generate_response_header_validator,
    'produces': generate_response_content_type_validator,
    'schema': generate_response_body_validator,
}


def generate_response_validator(api_path, operation_definition, response_definition,
                                path_definition, context):
    validators = ValidationDict()

    # Parameters is special cause it needs data from both the
    # `operation_definition` and the `path_definition`
    validators.add_validator('path', generate_path_validator(
        api_path=api_path,
        path_definition=path_definition,
        parameters=operation_definition.get('parameters', []),
        context=context,
    ))
    if '$ref' in response_definition:
        response_definition = dereference_reference(response_definition['$ref'], context)

    for key in validator_mapping:
        if key in response_definition:
            validators.add_validator(
                key,
                validator_mapping[key](context=context, **response_definition),
            )
        elif key in operation_definition:
            validators.add_validator(
                key,
                validator_mapping[key](context=context, **operation_definition),
            )
        elif key in path_definition:
            validators.add_validator(
                key,
                validator_mapping[key](context=context, **path_definition),
            )

    if 'produces' in context and 'produces' not in validators:
        validators.add_validator(
            'produces',
            generate_response_content_type_validator(**context),
        )

    return functools.partial(
        validate_object,
        field_validators=validators,
    )


def validate_response(response, request_method, schema):
    """
    Response validation involves the following steps.
       4. validate that the response status_code is in the allowed responses for
          the request method.
       5. validate that the response content validates against any provided
          schemas for the responses.
       6. headers, content-types, etc..., ???
    """
    with ErrorDict() as errors:
        # 1
        # TODO: tests
        try:
            api_path = validate_path_to_api_path(
                path=response.path,
                context=schema,
                **schema
            )
        except ValidationError as err:
            errors['path'].extend(list(err.messages))
            return  # this causes an exception to be raised since errors is no longer falsy.

        path_definition = schema['paths'][api_path] or {}

        # TODO: tests
        try:
            operation_definition = validate_request_method_to_operation(
                request_method=request_method,
                path_definition=path_definition,
            )
        except ValidationError as err:
            errors['method'].add_error(err.detail)
            return

        # 4
        try:
            response_definition = validate_status_code_to_response_definition(
                response=response,
                operation_definition=operation_definition,
            )
        except ValidationError as err:
            errors['status_code'].add_error(err.detail)
        else:
            # 5
            response_validator = generate_response_validator(
                api_path,
                operation_definition=operation_definition,
                path_definition=path_definition,
                response_definition=response_definition,
                context=schema,
            )
            try:
                response_validator(response, context=schema)
            except ValidationError as err:
                errors['body'].add_error(err.detail)
