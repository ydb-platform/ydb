# Copyright The OpenTracing Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools

from tornado.httpclient import HTTPRequest, HTTPError

import opentracing
from opentracing.ext import tags


g_tracing_disabled = True
g_client_tracer = None
g_start_span_cb = None


def _set_tracing_enabled(value):
    global g_tracing_disabled
    g_tracing_disabled = not value


def _set_tracing_info(tracer, start_span_cb):
    global g_client_tracer, g_start_span_cb
    g_client_tracer = tracer
    g_start_span_cb = start_span_cb


def _get_tracer():
    if g_client_tracer is None:
        return opentracing.tracer

    return g_client_tracer


def _normalize_request(args, kwargs):
    req = args[0]
    if not isinstance(req, str):
        # Not a string, no need to force the creation of a HTTPRequest
        return (args, kwargs)

    # keep the original kwargs for calling fetch()
    new_kwargs = {}
    for param in ('callback', 'raise_error'):
        if param in kwargs:
            new_kwargs[param] = kwargs.pop(param)

    req = HTTPRequest(req, **kwargs)
    new_args = [req]
    new_args.extend(args[1:])

    # return the normalized args/kwargs
    return (new_args, new_kwargs)


def fetch_async(func, handler, args, kwargs):
    # Return immediately if disabled, no args were provided (error)
    # or original_request is set (meaning we are in a redirect step).
    if g_tracing_disabled or \
            len(args) == 0 or hasattr(args[0], 'original_request'):
        return func(*args, **kwargs)

    # Force the creation of a HTTPRequest object if needed,
    # so we can inject the context into the headers.
    args, kwargs = _normalize_request(args, kwargs)
    request = args[0]

    tracer = _get_tracer()
    span = tracer.start_span(request.method)
    span.set_tag(tags.COMPONENT, 'tornado')
    span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
    span.set_tag(tags.HTTP_URL, request.url)
    span.set_tag(tags.HTTP_METHOD, request.method)

    tracer.inject(span.context,
                  opentracing.Format.HTTP_HEADERS,
                  request.headers)

    # Call the start_span_cb, if any.
    _call_start_span_cb(span, request)

    future = func(*args, **kwargs)

    # Finish the Span when the Future is done, making
    # sure the StackContext is restored (it's not by default).
    callback = functools.partial(_finish_tracing_callback, span=span)
    future.add_done_callback(callback)

    return future


def _finish_tracing_callback(future, span):

    status_code = None
    exc = future.exception()
    if exc:
        # Tornado uses HTTPError to report some of the
        # codes other than 2xx, so check the code is
        # actually in the 5xx range - and include the
        # status code for *all* HTTPError instances.
        error = True
        if isinstance(exc, HTTPError):
            status_code = exc.code
            if status_code < 500:
                error = False

        if error:
            span.set_tag(tags.ERROR, True)
            span.log_kv({
                'event': tags.ERROR,
                'error.object': exc,
            })
    else:
        status_code = future.result().code

    if status_code is not None:
        span.set_tag(tags.HTTP_STATUS_CODE, status_code)

    span.finish()


def _call_start_span_cb(span, request):
    if g_start_span_cb is None:
        return

    try:
        g_start_span_cb(span, request)
    except Exception:
        pass
