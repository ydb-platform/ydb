__all__ = [
    'SpecClassIdentificationError',
    'ApiError',
    'ValidationApiError',
    'InternalApiError',
    'AuthenticationApiError',
    'AccessDeniedApiError',
    'RemoteServiceUnavailableApiError',
    'DoesNotExistApiError',
    'ConflictStateApiError',
    'TooManyRequestsApiError',
    'IncorrectActionsApiError',
    'raise_on_api_error',
    'FailedOperation',
]

import json
from typing import Any, List, Optional

import attr
import httpx
from httpx import HTTPStatusError

from .error_codes import CommonErrorCodes, InternalErrorCodes
from ..util._docstrings import inherit_docstrings


# Client errors
@attr.attrs(auto_attribs=True, str=True, kw_only=True)
class SpecClassIdentificationError(Exception):
    """An exception that is raised when a specification сlass can't be find for a field specification name.

    Attributes:
        spec_field: The field specification name.
        spec_enum: An enumeration with all known specification names.
    """

    spec_field: Optional[str] = None
    spec_enum: Optional[str] = None


@attr.attrs(auto_attribs=True, str=True, kw_only=True)
class FailedOperation(Exception):
    """An exception that is raised when an operation fails.

    It could be raised when an inner operation fails.

    Attributes:
        operation: The instance of the failed operation.
    """
    operation: Optional[Any] = None


# API errors
@attr.attrs(auto_attribs=True, kw_only=True)
class ApiError(Exception):
    """A base class for exceptions that are raised when API methods return errors.

    Attributes:
        status_code: A HTTP response status code.
        request_id: The ID of the request that returns an error.
        code: An error code.
        message: An error description.
        payload: Additional data describing an error.
    """

    request_id: Optional[str] = None
    code: Optional[str] = None
    message: Optional[str] = None
    payload: Optional[Any] = None

    status_code: Optional[int] = None
    response: Optional[httpx.Response] = None

    def __str__(self):
        head = f'You have got a(n) {type(self).__name__} with http status code: {self.status_code}'
        code = f'Code of error: {self.code}'
        error_details = f'Error details: {self.message}'
        if self.payload:
            try:
                payload_str = json.dumps(self.payload, indent=4)
            except TypeError:
                payload_str = f'failed to parse payload as JSON! Falling back to raw representation:\n{self.payload}'
            additional_info = 'Additional information about the error:\n' + payload_str
        else:
            additional_info = ''
        request_id = f'request id: {self.request_id}. It needs to be specified when contacting support.'
        lines = [line for line in [head, code, error_details, additional_info, request_id] if line]
        result = '\n'.join(lines)
        return result


@inherit_docstrings
class ValidationApiError(ApiError):
    """An exception for a field validation error returned by an API method.

    Attributes:
        invalid_fields: A list of invalid fields.
    """

    _invalid_fields: Optional[List[str]] = None

    @property
    def invalid_fields(self) -> List[str]:
        if self._invalid_fields is None:
            self._invalid_fields = list(self.payload.keys())

        return self._invalid_fields


@inherit_docstrings
class InternalApiError(ApiError):
    pass


@inherit_docstrings
class AuthenticationApiError(ApiError):
    pass


@inherit_docstrings
class AccessDeniedApiError(ApiError):
    pass


@inherit_docstrings
class RemoteServiceUnavailableApiError(ApiError):
    pass


@inherit_docstrings
class DoesNotExistApiError(ApiError):
    pass


@inherit_docstrings
class ConflictStateApiError(ApiError):
    pass


@inherit_docstrings
class TooManyRequestsApiError(ApiError):
    pass


@inherit_docstrings
class IncorrectActionsApiError(ApiError):
    pass


_ERROR_MAP = {
    CommonErrorCodes.VALIDATION_ERROR.value: ValidationApiError,
    CommonErrorCodes.INTERNAL_ERROR.value: InternalApiError,
    CommonErrorCodes.AUTHENTICATION_ERROR.value: AuthenticationApiError,
    CommonErrorCodes.ACCESS_DENIED.value: AccessDeniedApiError,
    CommonErrorCodes.DOES_NOT_EXIST.value: DoesNotExistApiError,
    CommonErrorCodes.CONFLICT_STATE.value: ConflictStateApiError,
    CommonErrorCodes.TOO_MANY_REQUESTS.value: TooManyRequestsApiError,
    CommonErrorCodes.REMOTE_SERVICE_UNAVAILABLE.value: RemoteServiceUnavailableApiError,
    **{code.value: IncorrectActionsApiError for code in InternalErrorCodes}
}


def raise_on_api_error(response: httpx.Response):
    if 200 <= response.status_code < 300:
        return

    if not response.content:
        raise HTTPStatusError(message='No content', response=response, request=response.request)

    response_json = response.json()
    error_class = _ERROR_MAP.get(response_json['code'], ApiError)

    class_fields = [field.name for field in attr.fields(error_class)]
    for key in response_json.copy().keys():
        if key not in class_fields:
            response_json.pop(key)

    error = error_class(**response_json)
    error.status_code = response.status_code
    error.response = response
    raise error
