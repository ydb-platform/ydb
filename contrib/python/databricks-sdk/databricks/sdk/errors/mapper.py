import requests

from databricks.sdk.errors import platform
from databricks.sdk.errors.base import DatabricksError

from .overrides import _ALL_OVERRIDES


def _error_mapper(response: requests.Response, raw: dict) -> DatabricksError:
    for override in _ALL_OVERRIDES:
        if override.matches(response, raw):
            return override.custom_error(**raw)
    status_code = response.status_code
    error_code = raw.get("error_code", None)
    if error_code in platform.ERROR_CODE_MAPPING:
        # more specific error codes override more generic HTTP status codes
        return platform.ERROR_CODE_MAPPING[error_code](**raw)

    if status_code in platform.STATUS_CODE_MAPPING:
        # more generic HTTP status codes matched after more specific error codes,
        # where there's a default exception class per HTTP status code, and we do
        # rely on Databricks platform exception mapper to do the right thing.
        return platform.STATUS_CODE_MAPPING[status_code](**raw)

    # backwards-compatible error creation for cases like using older versions of
    # the SDK on way never releases of the platform.
    return DatabricksError(**raw)
