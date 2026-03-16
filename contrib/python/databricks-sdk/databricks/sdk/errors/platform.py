# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from .base import DatabricksError


class BadRequest(DatabricksError):
    """the request is invalid"""


class Unauthenticated(DatabricksError):
    """the request does not have valid authentication (AuthN) credentials for the operation"""


class PermissionDenied(DatabricksError):
    """the caller does not have permission to execute the specified operation"""


class NotFound(DatabricksError):
    """the operation was performed on a resource that does not exist"""


class ResourceConflict(DatabricksError):
    """maps to all HTTP 409 (Conflict) responses"""


class TooManyRequests(DatabricksError):
    """maps to HTTP code: 429 Too Many Requests"""


class Cancelled(DatabricksError):
    """the operation was explicitly canceled by the caller"""


class InternalError(DatabricksError):
    """some invariants expected by the underlying system have been broken"""


class NotImplemented(DatabricksError):
    """the operation is not implemented or is not supported/enabled in this service"""


class TemporarilyUnavailable(DatabricksError):
    """the service is currently unavailable"""


class DeadlineExceeded(DatabricksError):
    """the deadline expired before the operation could complete"""


class InvalidState(BadRequest):
    """unexpected state"""


class InvalidParameterValue(BadRequest):
    """supplied value for a parameter was invalid"""


class ResourceDoesNotExist(NotFound):
    """operation was performed on a resource that does not exist"""


class Aborted(ResourceConflict):
    """the operation was aborted, typically due to a concurrency issue such as a sequencer check
    failure"""


class AlreadyExists(ResourceConflict):
    """operation was rejected due a conflict with an existing resource"""


class ResourceAlreadyExists(ResourceConflict):
    """operation was rejected due a conflict with an existing resource"""


class ResourceExhausted(TooManyRequests):
    """operation is rejected due to per-user rate limiting"""


class RequestLimitExceeded(TooManyRequests):
    """cluster request was rejected because it would exceed a resource limit"""


class Unknown(InternalError):
    """this error is used as a fallback if the platform-side mapping is missing some reason"""


class DataLoss(InternalError):
    """unrecoverable data loss or corruption"""


STATUS_CODE_MAPPING = {
    400: BadRequest,
    401: Unauthenticated,
    403: PermissionDenied,
    404: NotFound,
    409: ResourceConflict,
    429: TooManyRequests,
    499: Cancelled,
    500: InternalError,
    501: NotImplemented,
    503: TemporarilyUnavailable,
    504: DeadlineExceeded,
}

ERROR_CODE_MAPPING = {
    "INVALID_STATE": InvalidState,
    "INVALID_PARAMETER_VALUE": InvalidParameterValue,
    "RESOURCE_DOES_NOT_EXIST": ResourceDoesNotExist,
    "ABORTED": Aborted,
    "ALREADY_EXISTS": AlreadyExists,
    "RESOURCE_ALREADY_EXISTS": ResourceAlreadyExists,
    "RESOURCE_EXHAUSTED": ResourceExhausted,
    "REQUEST_LIMIT_EXCEEDED": RequestLimitExceeded,
    "UNKNOWN": Unknown,
    "DATA_LOSS": DataLoss,
}
