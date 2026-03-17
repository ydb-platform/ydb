import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from . import details as errdetails


# Deprecated.
class ErrorDetail:
    def __init__(
        self,
        type: Optional[str] = None,
        reason: Optional[str] = None,
        domain: Optional[str] = None,
        metadata: Optional[dict] = None,
        **kwargs,
    ):
        self.type = type
        self.reason = reason
        self.domain = domain
        self.metadata = metadata

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ErrorDetail":
        # Key "@type" is not a valid keyword argument name in Python. Rename
        # it to "type" to avoid conflicts.
        safe_args = {}
        for k, v in d.items():
            safe_args[k if k != "@type" else "type"] = v

        return cls(**safe_args)


class DatabricksError(IOError):
    """Generic error from Databricks REST API"""

    def __init__(
        self,
        message: Optional[str] = None,
        *,
        error_code: Optional[str] = None,
        detail: Optional[str] = None,
        status: Optional[str] = None,
        scimType: Optional[str] = None,
        error: Optional[str] = None,
        retry_after_secs: Optional[int] = None,
        details: Optional[List[Dict[str, Any]]] = None,
        **kwargs,
    ):
        """

        :param message:
        :param error_code:
        :param detail: [Deprecated]
        :param status: [Deprecated]
        :param scimType: [Deprecated]
        :param error: [Deprecated]
        :param retry_after_secs: [Deprecated]
        :param details:
        :param kwargs:
        """

        if detail:
            # Handle SCIM error message details
            # @see https://tools.ietf.org/html/rfc7644#section-3.7.3
            if detail == "null":
                message = "SCIM API Internal Error"
            else:
                message = detail
            # add more context from SCIM responses
            message = f"{scimType} {message}".strip(" ")
            error_code = f"SCIM_{status}"

        super().__init__(message if message else error)
        self.error_code = error_code
        self.retry_after_secs = retry_after_secs
        self._error_details = errdetails.parse_error_details(details or [])
        self.kwargs = kwargs

        # Deprecated.
        self.details = []
        if details:
            for d in details:
                if not isinstance(d, dict):
                    continue
                self.details.append(ErrorDetail.from_dict(d))

    def get_error_info(self) -> List[ErrorDetail]:
        if self.details is None:
            return []
        return [detail for detail in self.details if detail.type == errdetails._ERROR_INFO_TYPE]

    def get_error_details(self) -> errdetails.ErrorDetails:
        return self._error_details


@dataclass
class _ErrorOverride:
    # The name of the override. Used for logging purposes.
    debug_name: str

    # A regex that must match the path of the request for this override to be applied.
    path_regex: re.Pattern

    # The HTTP method of the request for the override to apply
    verb: str

    # The custom error class to use for this override.
    custom_error: type

    # A regular expression that must match the error code for this override to be applied. If None,
    # this field is ignored.
    status_code_matcher: Optional[re.Pattern] = None

    # A regular expression that must match the error code for this override to be applied. If None,
    # this field is ignored.
    error_code_matcher: Optional[re.Pattern] = None

    # A regular expression that must match the message for this override to be applied. If None,
    # this field is ignored.
    message_matcher: Optional[re.Pattern] = None

    def matches(self, response: requests.Response, raw_error: dict):
        if response.request.method != self.verb:
            return False
        if not self.path_regex.match(response.request.path_url):
            return False
        if self.status_code_matcher and not self.status_code_matcher.match(str(response.status_code)):
            return False
        if self.error_code_matcher and not self.error_code_matcher.match(raw_error.get("error_code", "")):
            return False
        if self.message_matcher and not self.message_matcher.match(raw_error.get("message", "")):
            return False
        return True
