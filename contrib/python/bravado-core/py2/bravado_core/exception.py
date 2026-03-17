# -*- coding: utf-8 -*-
import sys

import six
import typing


if getattr(typing, 'TYPE_CHECKING', False):
    from bravado_core._compat_typing import FuncType


class SwaggerError(Exception):
    """Base exception class which all bravado-core specific exceptions
    inherit from.
    """


class SwaggerMappingError(SwaggerError):
    """Raised when an error is encountered during processing of a request or
    a response.
    """


class MatchingResponseNotFound(SwaggerMappingError):
    """Raised when an incoming or outgoing response cannot be matched to a
    documented response in the swagger spec.
    """


class SwaggerValidationError(SwaggerMappingError):
    """Raised when an error is encountered during validating user defined
    format values in a request or a response.
    """


class SwaggerSchemaError(SwaggerError):
    """Raised when an error is encountered during processing of a SwaggerSchema.
    """


class SwaggerSecurityValidationError(SwaggerValidationError):
    """Raised when an error is encountered during processing of
    security related Swagger definitions
    """


def wrap_exception(exception_class):
    # type: (typing.Type[BaseException]) -> typing.Callable[[FuncType], FuncType]
    """Helper decorator method to modify the raised exception class to
    `exception_class` but keeps the message and trace intact.

    :param exception_class: class to wrap raised exception with
    """
    def generic_exception(method):
        # type: (FuncType) -> FuncType
        def wrapper(*args, **kwargs):
            # type: (typing.Any, typing.Any) -> typing.Any
            try:
                return method(*args, **kwargs)
            except Exception as e:
                six.reraise(
                    exception_class,
                    exception_class(str(e)),
                    sys.exc_info()[2],
                )
        return wrapper  # type: ignore  # ignoring type to avoiding typing.cast call

    return generic_exception
