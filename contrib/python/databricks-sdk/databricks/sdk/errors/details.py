import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ErrorInfo:
    """Describes the cause of the error with structured details."""

    reason: str
    domain: str
    metadata: Dict[str, str]


@dataclass
class RequestInfo:
    """
    Contains metadata about the request that clients can attach when
    filing a bug or providing other forms of feedback.
    """

    request_id: str
    serving_data: str


@dataclass
class RetryInfo:
    """
    Describes when the clients can retry a failed request. Clients could
    ignore the recommendation here or retry when this information is missing
    from error responses.

    It's always recommended that clients should use exponential backoff
    when retrying.

    Clients should wait until `retry_delay` amount of time has passed since
    receiving the error response before retrying.  If retrying requests also
    fail, clients should use an exponential backoff scheme to gradually
    increase the delay between retries based on `retry_delay`, until either
    a maximum number of retries have been reached or a maximum retry delay
    cap has been reached.
    """

    retry_delay_seconds: float


@dataclass
class DebugInfo:
    """Describes additional debugging info."""

    stack_entries: List[str]
    detail: str


@dataclass
class QuotaFailureViolation:
    """Describes a single quota violation."""

    subject: str
    description: str


@dataclass
class QuotaFailure:
    """
    Describes how a quota check failed.

    For example if a daily limit was exceeded for the calling project, a
    service could respond with a QuotaFailure detail containing the project
    id and the description of the quota limit that was exceeded.  If the
    calling project hasn't enabled the service in the developer console,
    then a service could respond with the project id and set
    `service_disabled` to true.

    Also see RetryInfo and Help types for other details about handling a
    quota failure.
    """

    violations: List[QuotaFailureViolation]


@dataclass
class PreconditionFailureViolation:
    """Describes a single precondition violation."""

    type: str
    subject: str
    description: str


@dataclass
class PreconditionFailure:
    """Describes what preconditions have failed."""

    violations: List[PreconditionFailureViolation]


@dataclass
class BadRequestFieldViolation:
    """Describes a single field violation in a bad request."""

    field: str
    description: str


@dataclass
class BadRequest:
    """
    Describes violations in a client request. This error type
    focuses on the syntactic aspects of the request.
    """

    field_violations: List[BadRequestFieldViolation]


@dataclass
class ResourceInfo:
    """Describes the resource that is being accessed."""

    resource_type: str
    resource_name: str
    owner: str
    description: str


@dataclass
class HelpLink:
    """Describes a single help link."""

    description: str
    url: str


@dataclass
class Help:
    """
    Provides links to documentation or for performing an out of
    band action.

    For example, if a quota check failed with an error indicating
    the calling project hasn't enabled the accessed service, this
    can contain a URL pointing directly to the right place in the
    developer console to flip the bit.
    """

    links: List[HelpLink]


@dataclass
class ErrorDetails:
    """
    ErrorDetails contains the error details of an API error. It
    is the union of known error details types and unknown details.
    """

    error_info: Optional[ErrorInfo] = None
    request_info: Optional[RequestInfo] = None
    retry_info: Optional[RetryInfo] = None
    debug_info: Optional[DebugInfo] = None
    quota_failure: Optional[QuotaFailure] = None
    precondition_failure: Optional[PreconditionFailure] = None
    bad_request: Optional[BadRequest] = None
    resource_info: Optional[ResourceInfo] = None
    help: Optional[Help] = None
    unknown_details: List[Any] = field(default_factory=list)


# Supported error details proto types.
_ERROR_INFO_TYPE = "type.googleapis.com/google.rpc.ErrorInfo"
_REQUEST_INFO_TYPE = "type.googleapis.com/google.rpc.RequestInfo"
_RETRY_INFO_TYPE = "type.googleapis.com/google.rpc.RetryInfo"
_DEBUG_INFO_TYPE = "type.googleapis.com/google.rpc.DebugInfo"
_QUOTA_FAILURE_TYPE = "type.googleapis.com/google.rpc.QuotaFailure"
_PRECONDITION_FAILURE_TYPE = "type.googleapis.com/google.rpc.PreconditionFailure"
_BAD_REQUEST_TYPE = "type.googleapis.com/google.rpc.BadRequest"
_RESOURCE_INFO_TYPE = "type.googleapis.com/google.rpc.ResourceInfo"
_HELP_TYPE = "type.googleapis.com/google.rpc.Help"


def parse_error_details(details: List[Any]) -> ErrorDetails:
    ed = ErrorDetails()

    if not details:
        return ed

    for d in details:
        pd = _parse_json_error_details(d)

        if isinstance(pd, ErrorInfo):
            ed.error_info = pd
        elif isinstance(pd, RequestInfo):
            ed.request_info = pd
        elif isinstance(pd, RetryInfo):
            ed.retry_info = pd
        elif isinstance(pd, DebugInfo):
            ed.debug_info = pd
        elif isinstance(pd, QuotaFailure):
            ed.quota_failure = pd
        elif isinstance(pd, PreconditionFailure):
            ed.precondition_failure = pd
        elif isinstance(pd, BadRequest):
            ed.bad_request = pd
        elif isinstance(pd, ResourceInfo):
            ed.resource_info = pd
        elif isinstance(pd, Help):
            ed.help = pd
        else:
            ed.unknown_details.append(pd)

    return ed


def _parse_json_error_details(value: Any) -> Any:
    """
    Attempts to parse an error details type from the given JSON value. If the
    value is not a known error details type, it returns the input as is.

    :param value: The JSON value to parse.
    :return: The parsed error details type or the input value if it is not
        a known error details type.
    """

    if not isinstance(value, dict):
        return value  # not a JSON object

    t = value.get("@type")
    if not isinstance(t, str):
        return value  # JSON object with no @type field

    try:
        if t == _ERROR_INFO_TYPE:
            return _parse_error_info(value)
        elif t == _REQUEST_INFO_TYPE:
            return _parse_req_info(value)
        elif t == _RETRY_INFO_TYPE:
            return _parse_retry_info(value)
        elif t == _DEBUG_INFO_TYPE:
            return _parse_debug_info(value)
        elif t == _QUOTA_FAILURE_TYPE:
            return _parse_quota_failure(value)
        elif t == _PRECONDITION_FAILURE_TYPE:
            return _parse_precondition_failure(value)
        elif t == _BAD_REQUEST_TYPE:
            return _parse_bad_request(value)
        elif t == _RESOURCE_INFO_TYPE:
            return _parse_resource_info(value)
        elif t == _HELP_TYPE:
            return _parse_help(value)
        else:  # unknown type
            return value
    except (TypeError, ValueError):
        return value  # not a valid known type
    except Exception:
        return value


def _parse_error_info(d: Dict[str, Any]) -> ErrorInfo:
    return ErrorInfo(
        domain=_parse_string(d.get("domain", "")),
        reason=_parse_string(d.get("reason", "")),
        metadata=_parse_dict(d.get("metadata", {})),
    )


def _parse_req_info(d: Dict[str, Any]) -> RequestInfo:
    return RequestInfo(
        request_id=_parse_string(d.get("request_id", "")),
        serving_data=_parse_string(d.get("serving_data", "")),
    )


def _parse_retry_info(d: Dict[str, Any]) -> RetryInfo:
    delay = 0.0
    if "retry_delay" in d:
        delay = _parse_seconds(d["retry_delay"])

    return RetryInfo(
        retry_delay_seconds=delay,
    )


def _parse_debug_info(d: Dict[str, Any]) -> DebugInfo:
    di = DebugInfo(
        stack_entries=[],
        detail=_parse_string(d.get("detail", "")),
    )

    if "stack_entries" not in d:
        return di

    if not isinstance(d["stack_entries"], list):
        raise ValueError(f"Expected list, got {d['stack_entries']!r}")
    for entry in d["stack_entries"]:
        di.stack_entries.append(_parse_string(entry))

    return di


def _parse_quota_failure_violation(d: Dict[str, Any]) -> QuotaFailureViolation:
    return QuotaFailureViolation(
        subject=_parse_string(d.get("subject", "")),
        description=_parse_string(d.get("description", "")),
    )


def _parse_quota_failure(d: Dict[str, Any]) -> QuotaFailure:
    violations = []
    if "violations" in d:
        if not isinstance(d["violations"], list):
            raise ValueError(f"Expected list, got {d['violations']!r}")
        for violation in d["violations"]:
            if not isinstance(violation, dict):
                raise ValueError(f"Expected dict, got {violation!r}")
            violations.append(_parse_quota_failure_violation(violation))
    return QuotaFailure(violations=violations)


def _parse_precondition_failure_violation(d: Dict[str, Any]) -> PreconditionFailureViolation:
    return PreconditionFailureViolation(
        type=_parse_string(d.get("type", "")),
        subject=_parse_string(d.get("subject", "")),
        description=_parse_string(d.get("description", "")),
    )


def _parse_precondition_failure(d: Dict[str, Any]) -> PreconditionFailure:
    violations = []
    if "violations" in d:
        if not isinstance(d["violations"], list):
            raise ValueError(f"Expected list, got {d['violations']!r}")
        for v in d["violations"]:
            if not isinstance(v, dict):
                raise ValueError(f"Expected dict, got {v!r}")
            violations.append(_parse_precondition_failure_violation(v))
    return PreconditionFailure(violations=violations)


def _parse_bad_request_field_violation(d: Dict[str, Any]) -> BadRequestFieldViolation:
    return BadRequestFieldViolation(
        field=_parse_string(d.get("field", "")),
        description=_parse_string(d.get("description", "")),
    )


def _parse_bad_request(d: Dict[str, Any]) -> BadRequest:
    field_violations = []
    if "field_violations" in d:
        if not isinstance(d["field_violations"], list):
            raise ValueError(f"Expected list, got {d['field_violations']!r}")
        for violation in d["field_violations"]:
            if not isinstance(violation, dict):
                raise ValueError(f"Expected dict, got {violation!r}")
            field_violations.append(_parse_bad_request_field_violation(violation))
    return BadRequest(field_violations=field_violations)


def _parse_resource_info(d: Dict[str, Any]) -> ResourceInfo:
    return ResourceInfo(
        resource_type=_parse_string(d.get("resource_type", "")),
        resource_name=_parse_string(d.get("resource_name", "")),
        owner=_parse_string(d.get("owner", "")),
        description=_parse_string(d.get("description", "")),
    )


def _parse_help_link(d: Dict[str, Any]) -> HelpLink:
    return HelpLink(
        description=_parse_string(d.get("description", "")),
        url=_parse_string(d.get("url", "")),
    )


def _parse_help(d: Dict[str, Any]) -> Help:
    links = []
    if "links" in d:
        if not isinstance(d["links"], list):
            raise ValueError(f"Expected list, got {d['links']!r}")
        for link in d["links"]:
            if not isinstance(link, dict):
                raise ValueError(f"Expected dict, got {link!r}")
            links.append(_parse_help_link(link))
    return Help(links=links)


def _parse_string(a: Any) -> str:
    if isinstance(a, str):
        return a
    raise ValueError(f"Expected string, got {a!r}")


def _parse_dict(a: Any) -> Dict[str, str]:
    if not isinstance(a, dict):
        raise ValueError(f"Expected Dict[str, str], got {a!r}")
    for key, value in a.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError(f"Expected Dict[str, str], got {a!r}")
    return a


def _parse_seconds(a: Any) -> float:
    """
    Parse a duration string into a float representing the number of seconds.

    The duration type is encoded as a string rather than an where the string
    ends in the suffix "s" (indicating seconds) and is preceded by a decimal
    number of seconds. For example, "3.000000001s", represents a duration of
    3 seconds and 1 nanosecond.
    """

    if not isinstance(a, str):
        raise ValueError(f"Expected string, got {a!r}")

    match = re.match(r"^(\d+(\.\d+)?)s$", a)
    if match:
        return float(match.group(1))

    raise ValueError(f"Expected duration string, got {a!r}")
