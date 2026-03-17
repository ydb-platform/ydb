import time
from typing import Any, Dict, List, NamedTuple, Optional

from .mypy_types import Headers, OptBool, OptInt, OptStr, OptTs


# possible span kinds
CLIENT = "CLIENT"
SERVER = "SERVER"
PRODUCER = "PRODUCER"
CONSUMER = "CONSUMER"

# zipkin headers, for more information see:
# https://github.com/openzipkin/b3-propagation

TRACE_ID_HEADER = "X-B3-TraceId"
SPAN_ID_HEADER = "X-B3-SpanId"
PARENT_ID_HEADER = "X-B3-ParentSpanId"
FLAGS_HEADER = "X-B3-Flags"
SAMPLED_ID_HEADER = "X-B3-Sampled"
SINGLE_HEADER = "b3"
DELIMITER = "-"
DEBUG_MARKER = "d"


class _TraceContext(NamedTuple):
    trace_id: str
    parent_id: OptStr
    span_id: str
    sampled: OptBool
    debug: bool
    shared: bool


class TraceContext(_TraceContext):
    """Immutable class with trace related data that travels across
    process boundaries.
    """

    def make_headers(self) -> Headers:
        """Creates dict with zipkin headers from available context.

        Resulting dict should be passed to HTTP client  propagate contest
        to other services.
        """
        return make_headers(self)

    def make_single_header(self) -> Headers:
        return make_single_header(self)


class Endpoint(NamedTuple):
    serviceName: OptStr
    ipv4: OptStr
    ipv6: OptStr
    port: OptInt


def create_endpoint(
    service_name: str, *, ipv4: OptStr = None, ipv6: OptStr = None, port: OptInt = None
) -> Endpoint:
    """Factory function to create Endpoint object."""
    return Endpoint(service_name, ipv4, ipv6, port)


def make_timestamp(ts: OptTs = None) -> int:
    """Create zipkin timestamp in microseconds, or convert available one
    from second. Useful when user supplies ts from time.time() call.
    """
    ts = ts if ts is not None else time.time()
    return int(ts * 1000 * 1000)  # microseconds


def make_headers(context: TraceContext) -> Headers:
    """Creates dict with zipkin headers from supplied trace context."""
    headers = {
        TRACE_ID_HEADER: context.trace_id,
        SPAN_ID_HEADER: context.span_id,
        FLAGS_HEADER: "0",
        SAMPLED_ID_HEADER: "1" if context.sampled else "0",
    }
    if context.parent_id is not None:
        headers[PARENT_ID_HEADER] = context.parent_id
    return headers


def make_single_header(context: TraceContext) -> Headers:
    """Creates dict with zipkin single header format."""
    # b3={TraceId}-{SpanId}-{SamplingState}-{ParentSpanId}
    c = context

    # encode sampled flag
    if c.debug:
        sampled = "d"
    elif c.sampled:
        sampled = "1"
    else:
        sampled = "0"

    params: List[str] = [c.trace_id, c.span_id, sampled]
    if c.parent_id is not None:
        params.append(c.parent_id)

    h = DELIMITER.join(params)
    headers = {SINGLE_HEADER: h}
    return headers


def parse_sampled_header(headers: Headers) -> OptBool:
    sampled = headers.get(SAMPLED_ID_HEADER.lower(), None)
    if sampled is None or sampled == "":
        return None
    return True if sampled == "1" else False


def parse_debug_header(headers: Headers) -> bool:
    return True if headers.get(FLAGS_HEADER, "0") == "1" else False


def _parse_parent_id(parts: List[str]) -> OptStr:
    # parse parent_id part from zipkin single header propagation
    parent_id = None
    if len(parts) >= 4:
        parent_id = parts[3]
    return parent_id


def _parse_debug(parts: List[str]) -> bool:
    # parse debug part from zipkin single header propagation
    debug = False
    if len(parts) >= 3 and parts[2] == DEBUG_MARKER:
        debug = True
    return debug


def _parse_sampled(parts: List[str]) -> OptBool:
    # parse sampled part from zipkin single header propagation
    sampled: OptBool = None
    if len(parts) >= 3:
        if parts[2] in ("1", "0"):
            sampled = bool(int(parts[2]))
    return sampled


def _parse_single_header(headers: Headers) -> Optional[TraceContext]:
    # Makes TraceContext from zipkin single header format.
    # https://github.com/openzipkin/b3-propagation

    # b3={TraceId}-{SpanId}-{SamplingState}-{ParentSpanId}
    if headers[SINGLE_HEADER] == "0":
        return None
    payload = headers[SINGLE_HEADER].lower()
    parts: List[str] = payload.split(DELIMITER)
    if len(parts) < 2:
        return None

    debug = _parse_debug(parts)
    sampled = debug if debug else _parse_sampled(parts)

    context = TraceContext(
        trace_id=parts[0],
        span_id=parts[1],
        parent_id=_parse_parent_id(parts),
        sampled=sampled,
        debug=debug,
        shared=False,
    )
    return context


def make_context(headers: Headers) -> Optional[TraceContext]:
    """Converts available headers to TraceContext, if headers mapping does
    not contain zipkin headers, function returns None.
    """
    # TODO: add validation for trace_id/span_id/parent_id

    # normalize header names just in case someone passed regular dict
    # instead dict with case insensitive keys
    headers = {k.lower(): v for k, v in headers.items()}

    required = (TRACE_ID_HEADER.lower(), SPAN_ID_HEADER.lower())
    has_b3 = all(h in headers for h in required)
    has_b3_single = SINGLE_HEADER in headers

    if not (has_b3_single or has_b3):
        return None

    if has_b3:
        debug = parse_debug_header(headers)
        sampled = debug if debug else parse_sampled_header(headers)
        context = TraceContext(
            trace_id=headers[TRACE_ID_HEADER.lower()],
            parent_id=headers.get(PARENT_ID_HEADER.lower()),
            span_id=headers[SPAN_ID_HEADER.lower()],
            sampled=sampled,
            debug=debug,
            shared=False,
        )
        return context
    return _parse_single_header(headers)


OptKeys = Optional[List[str]]


def filter_none(data: Dict[str, Any], keys: OptKeys = None) -> Dict[str, Any]:
    """Filter keys from dict with None values.

    Check occurs only on root level. If list of keys specified, filter
    works only for selected keys
    """

    def limited_filter(k: str, v: Any) -> bool:
        return k not in keys or v is not None  # type: ignore

    def full_filter(k: str, v: Any) -> bool:
        return v is not None

    f = limited_filter if keys is not None else full_filter
    return {k: v for k, v in data.items() if f(k, v)}
