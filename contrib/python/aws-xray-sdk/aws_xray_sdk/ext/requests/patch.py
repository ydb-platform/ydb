import wrapt

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.models import http
from aws_xray_sdk.ext.util import inject_trace_header, strip_url, get_hostname


def patch():

    wrapt.wrap_function_wrapper(
        'requests',
        'Session.request',
        _xray_traced_requests
    )

    wrapt.wrap_function_wrapper(
        'requests',
        'Session.prepare_request',
        _inject_header
    )


def _xray_traced_requests(wrapped, instance, args, kwargs):

    url = kwargs.get('url') or args[1]

    return xray_recorder.record_subsegment(
        wrapped, instance, args, kwargs,
        name=get_hostname(url),
        namespace='remote',
        meta_processor=requests_processor,
    )


def _inject_header(wrapped, instance, args, kwargs):
    request = args[0]
    headers = getattr(request, 'headers', {})
    inject_trace_header(headers, xray_recorder.current_subsegment())
    setattr(request, 'headers', headers)

    return wrapped(*args, **kwargs)


def requests_processor(wrapped, instance, args, kwargs,
                       return_value, exception, subsegment, stack):

    method = kwargs.get('method') or args[0]
    url = kwargs.get('url') or args[1]

    subsegment.put_http_meta(http.METHOD, method)
    subsegment.put_http_meta(http.URL, strip_url(url))

    if return_value is not None:
        subsegment.put_http_meta(http.STATUS, return_value.status_code)
    elif exception:
        subsegment.add_exception(exception, stack)
