import re
from urllib.parse import urlparse

import wrapt

from aws_xray_sdk.core.models import http
from aws_xray_sdk.core.models.trace_header import TraceHeader

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
UNKNOWN_HOSTNAME = "UNKNOWN HOST"


def inject_trace_header(headers, entity):
    """
    Extract trace id, entity id and sampling decision
    from the input entity and inject these information
    to headers.

    :param dict headers: http headers to inject
    :param Entity entity: trace entity that the trace header
        value generated from.
    """
    if not entity:
        return

    if hasattr(entity, 'type') and entity.type == 'subsegment':
        header = entity.parent_segment.get_origin_trace_header()
    else:
        header = entity.get_origin_trace_header()
    data = header.data if header else None
    to_insert = TraceHeader(
        root=entity.trace_id,
        parent=entity.id,
        sampled=entity.sampled,
        data=data,
    )

    value = to_insert.to_header_str()

    headers[http.XRAY_HEADER] = value


def calculate_sampling_decision(trace_header, recorder, sampling_req):
    """
    Return 1 or the matched rule name if should sample and 0 if should not.
    The sampling decision coming from ``trace_header`` always has
    the highest precedence. If the ``trace_header`` doesn't contain
    sampling decision then it checks if sampling is enabled or not
    in the recorder. If not enbaled it returns 1. Otherwise it uses user
    defined sampling rules to decide.
    """
    if trace_header.sampled is not None and trace_header.sampled != '?':
        return trace_header.sampled
    elif not recorder.sampling:
        return 1
    else:
        decision = recorder.sampler.should_trace(sampling_req)
    return decision if decision else 0


def construct_xray_header(headers):
    """
    Construct a ``TraceHeader`` object from dictionary headers
    of the incoming request. This method should always return
    a ``TraceHeader`` object regardless of tracing header's presence
    in the incoming request.
    """
    header_str = headers.get(http.XRAY_HEADER) or headers.get(http.ALT_XRAY_HEADER)
    if header_str:
        return TraceHeader.from_header_str(header_str)
    else:
        return TraceHeader()


def calculate_segment_name(host_name, recorder):
    """
    Returns the segment name based on recorder configuration and
    input host name. This is a helper generally used in web framework
    middleware where a host name is available from incoming request's headers.
    """
    if recorder.dynamic_naming:
        return recorder.dynamic_naming.get_name(host_name)
    else:
        return recorder.service


def prepare_response_header(origin_header, segment):
    """
    Prepare a trace header to be inserted into response
    based on original header and the request segment.
    """
    if origin_header and origin_header.sampled == '?':
        new_header = TraceHeader(root=segment.trace_id,
                                 sampled=segment.sampled)
    else:
        new_header = TraceHeader(root=segment.trace_id)

    return new_header.to_header_str()


def to_snake_case(name):
    """
    Convert the input string to snake-cased string.
    """
    s1 = first_cap_re.sub(r'\1_\2', name)
    # handle acronym words
    return all_cap_re.sub(r'\1_\2', s1).lower()


# ? is not a valid entity, and we don't want things after the ? for the segment name
def strip_url(url):
    """
    Will generate a valid url string for use as a segment name
    :param url: url to strip
    :return: validated url string
    """
    return url.partition('?')[0] if url else url


def get_hostname(url):
    if url is None:
        return UNKNOWN_HOSTNAME
    url_parse = urlparse(url)
    hostname = url_parse.hostname
    if hostname is None:
        return UNKNOWN_HOSTNAME
    return hostname if hostname else url  # If hostname is none, we return the regular URL; indication of malformed url


def unwrap(obj, attr):
    """
    Will unwrap a `wrapt` attribute
    :param obj: base object
    :param attr: attribute on `obj` to unwrap
    """
    f = getattr(obj, attr, None)
    if f and isinstance(f, wrapt.ObjectProxy) and hasattr(f, '__wrapped__'):
        setattr(obj, attr, f.__wrapped__)
