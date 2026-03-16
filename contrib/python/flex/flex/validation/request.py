from flex.exceptions import ValidationError
from flex.context_managers import ErrorDict
from flex.validation.operation import (
    construct_operation_validators,
    validate_operation,
)
from flex.validation.common import (
    validate_request_method_to_operation,
    validate_path_to_api_path,
)


def validate_request(request, schema):
    """
    Request validation does the following steps.

       1. validate that the path matches one of the defined paths in the schema.
       2. validate that the request method conforms to a supported methods for the given path.
       3. validate that the request parameters conform to the parameter
          definitions for the operation definition.
    """
    with ErrorDict() as errors:
        # 1
        try:
            api_path = validate_path_to_api_path(
                path=request.path,
                context=schema,
                **schema
            )
        except ValidationError as err:
            errors['path'].add_error(err.detail)
            return  # this causes an exception to be raised since errors is no longer falsy.

        path_definition = schema['paths'][api_path] or {}

        if not path_definition:
            # TODO: is it valid to not have a definition for a path?
            return

        # 2
        try:
            operation_definition = validate_request_method_to_operation(
                request_method=request.method,
                path_definition=path_definition,
            )
        except ValidationError as err:
            errors['method'].add_error(err.detail)
            return

        if operation_definition is None:
            # TODO: is this compliant with swagger, can path operations have a null
            # definition?
            return

        # 3
        operation_validators = construct_operation_validators(
            api_path=api_path,
            path_definition=path_definition,
            operation_definition=operation_definition,
            context=schema,
        )
        try:
            validate_operation(request, operation_validators, context=schema)
        except ValidationError as err:
            errors['method'].add_error(err.detail)
