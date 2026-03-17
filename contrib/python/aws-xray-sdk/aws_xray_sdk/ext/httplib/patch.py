import fnmatch
from collections import namedtuple

import urllib3.connection
import wrapt

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.exceptions.exceptions import SegmentNotFoundException
from aws_xray_sdk.core.models import http
from aws_xray_sdk.core.patcher import _PATCHED_MODULES
from aws_xray_sdk.ext.util import get_hostname, inject_trace_header, strip_url, unwrap

httplib_client_module = 'http.client'
import http.client as httplib

_XRAY_PROP = '_xray_prop'
_XRay_Data = namedtuple('xray_data', ['method', 'host', 'url'])
_XRay_Ignore = namedtuple('xray_ignore', ['subclass', 'hostname', 'urls'])
# A flag indicates whether this module is X-Ray patched or not
PATCH_FLAG = '__xray_patched'
# Calls that should be ignored
_XRAY_IGNORE = set()


def add_ignored(subclass=None, hostname=None, urls=None):
    global _XRAY_IGNORE
    if subclass is not None or hostname is not None or urls is not None:
        urls = urls if urls is None else tuple(urls)
        _XRAY_IGNORE.add(_XRay_Ignore(subclass=subclass, hostname=hostname, urls=urls))


def reset_ignored():
    global _XRAY_IGNORE
    _XRAY_IGNORE.clear()
    _ignored_add_default()


def _ignored_add_default():
    # skip httplib tracing for SDK built-in centralized sampling pollers
    add_ignored(subclass='botocore.awsrequest.AWSHTTPConnection', urls=['/GetSamplingRules', '/SamplingTargets'])


# make sure we have the default rules
_ignored_add_default()


def http_response_processor(wrapped, instance, args, kwargs, return_value,
                            exception, subsegment, stack):
    xray_data = getattr(instance, _XRAY_PROP, None)
    if not xray_data:
        return

    subsegment.put_http_meta(http.METHOD, xray_data.method)
    subsegment.put_http_meta(http.URL, strip_url(xray_data.url))

    if return_value:
        subsegment.put_http_meta(http.STATUS, return_value.status)

        # propagate to response object
        xray_data = _XRay_Data('READ', xray_data.host, xray_data.url)
        setattr(return_value, _XRAY_PROP, xray_data)

    if exception:
        subsegment.add_exception(exception, stack)


def _xray_traced_http_getresponse(wrapped, instance, args, kwargs):
    xray_data = getattr(instance, _XRAY_PROP, None)
    if not xray_data:
        return wrapped(*args, **kwargs)

    return xray_recorder.record_subsegment(
        wrapped, instance, args, kwargs,
        name=get_hostname(xray_data.url),
        namespace='remote',
        meta_processor=http_response_processor,
    )


def http_send_request_processor(wrapped, instance, args, kwargs, return_value,
                                exception, subsegment, stack):
    xray_data = getattr(instance, _XRAY_PROP, None)
    if not xray_data:
        return

    # we don't delete the attr as we can have multiple reads
    subsegment.put_http_meta(http.METHOD, xray_data.method)
    subsegment.put_http_meta(http.URL, strip_url(xray_data.url))

    if exception:
        subsegment.add_exception(exception, stack)


def _ignore_request(instance, hostname, url):
    global _XRAY_IGNORE
    module = instance.__class__.__module__
    if module is None or module == str.__class__.__module__:
        subclass = instance.__class__.__name__
    else:
        subclass = module + '.' + instance.__class__.__name__
    for rule in _XRAY_IGNORE:
        subclass_match = subclass == rule.subclass if rule.subclass is not None else True
        host_match = fnmatch.fnmatch(hostname, rule.hostname) if rule.hostname is not None else True
        url_match = url in rule.urls if rule.urls is not None else True
        if url_match and host_match and subclass_match:
            return True
    return False


def _send_request(wrapped, instance, args, kwargs):
    def decompose_args(method, url, body, headers, encode_chunked=False):
        # skip any ignored requests
        if _ignore_request(instance, instance.host, url):
            return wrapped(*args, **kwargs)

        # Only injects headers when the subsegment for the outgoing
        # calls are opened successfully.
        subsegment = None
        try:
            subsegment = xray_recorder.current_subsegment()
        except SegmentNotFoundException:
            pass
        if subsegment:
            inject_trace_header(headers, subsegment)

        if issubclass(instance.__class__, urllib3.connection.HTTPSConnection):
            ssl_cxt = getattr(instance, 'ssl_context', None)
        elif issubclass(instance.__class__, httplib.HTTPSConnection):
            ssl_cxt = getattr(instance, '_context', None)
        else:
            # In this case, the patcher can't determine which module the connection instance is from.
            # We default to it to check ssl_context but may be None so that the default scheme would be
            # (and may falsely be) http.
            ssl_cxt = getattr(instance, 'ssl_context', None)
        scheme = 'https' if ssl_cxt and type(ssl_cxt).__name__ == 'SSLContext' else 'http'
        xray_url = '{}://{}{}'.format(scheme, instance.host, url)
        xray_data = _XRay_Data(method, instance.host, xray_url)
        setattr(instance, _XRAY_PROP, xray_data)

        # we add a segment here in case connect fails
        return xray_recorder.record_subsegment(
            wrapped, instance, args, kwargs,
            name=get_hostname(xray_data.url),
            namespace='remote',
            meta_processor=http_send_request_processor
        )

    return decompose_args(*args, **kwargs)


def http_read_processor(wrapped, instance, args, kwargs, return_value,
                        exception, subsegment, stack):
    xray_data = getattr(instance, _XRAY_PROP, None)
    if not xray_data:
        return

    # we don't delete the attr as we can have multiple reads
    subsegment.put_http_meta(http.METHOD, xray_data.method)
    subsegment.put_http_meta(http.URL, strip_url(xray_data.url))
    subsegment.put_http_meta(http.STATUS, instance.status)

    if exception:
        subsegment.add_exception(exception, stack)


def _xray_traced_http_client_read(wrapped, instance, args, kwargs):
    xray_data = getattr(instance, _XRAY_PROP, None)
    if not xray_data:
        return wrapped(*args, **kwargs)

    return xray_recorder.record_subsegment(
        wrapped, instance, args, kwargs,
        name=get_hostname(xray_data.url),
        namespace='remote',
        meta_processor=http_read_processor
    )


def patch():
    """
    patch the built-in `urllib/httplib/httplib.client` methods for tracing.
    """
    if getattr(httplib, PATCH_FLAG, False):
        return
    # we set an attribute to avoid multiple wrapping
    setattr(httplib, PATCH_FLAG, True)

    wrapt.wrap_function_wrapper(
        httplib_client_module,
        'HTTPConnection._send_request',
        _send_request
    )

    wrapt.wrap_function_wrapper(
        httplib_client_module,
        'HTTPConnection.getresponse',
        _xray_traced_http_getresponse
    )

    wrapt.wrap_function_wrapper(
        httplib_client_module,
        'HTTPResponse.read',
        _xray_traced_http_client_read
    )


def unpatch():
    """
    Unpatch any previously patched modules.
    This operation is idempotent.
    """
    _PATCHED_MODULES.discard('httplib')
    setattr(httplib, PATCH_FLAG, False)
    # _send_request encapsulates putrequest, putheader[s], and endheaders
    unwrap(httplib.HTTPConnection, '_send_request')
    unwrap(httplib.HTTPConnection, 'getresponse')
    unwrap(httplib.HTTPResponse, 'read')
