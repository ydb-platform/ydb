import functools

from flex.validation.parameter import (
    validate_path_parameters,
)


def generate_path_parameters_validator(api_path, path_parameters, context):
    """
    Generates a validator function that given a path, validates that it against
    the path parameters
    """
    path_parameter_validator = functools.partial(
        validate_path_parameters,
        api_path=api_path,
        path_parameters=path_parameters,
        context=context,
    )
    return path_parameter_validator
