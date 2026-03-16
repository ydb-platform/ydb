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
import wrapt

import opentracing
from opentracing.ext import tags
from opentracing.scope_managers.tornado import tracer_stack_context

from ._constants import SCOPE_ATTR


class TornadoTracing(object):
    """
    @param tracer the OpenTracing tracer to be used
    to trace requests using this TornadoTracing
    """
    def __init__(self, tracer=None, start_span_cb=None):
        if start_span_cb is not None and not callable(start_span_cb):
            raise ValueError('start_span_cb is not callable')

        self._tracer_obj = tracer
        self._start_span_cb = start_span_cb
        self._trace_all = False
        self._trace_client = False

    @property
    def _tracer(self):
        """
        DEPRECATED
        """
        return self.tracer

    @property
    def tracer(self):
        if self._tracer_obj is None:
            return opentracing.tracer

        return self._tracer_obj

    def get_span(self, request):
        """
        @param request
        Returns the span tracing this request
        """
        scope = getattr(request, SCOPE_ATTR, None)
        return None if scope is None else scope.span

    def trace(self, *attributes):
        """
        Function decorator that traces functions
        NOTE: Must be placed before the Tornado decorators
        @param attributes any number of request attributes
        (strings) to be set as tags on the created span
        """

        @wrapt.decorator
        def wrapper(wrapped, instance, args, kwargs):
            if self._trace_all:
                return wrapped(*args, **kwargs)

            handler = instance

            with tracer_stack_context():
                try:
                    self._apply_tracing(handler, list(attributes))

                    # Run the actual function.
                    result = wrapped(*args, **kwargs)

                    # if it has `add_done_callback` it's a Future,
                    # else, a normal method/function.
                    if callable(getattr(result, 'add_done_callback', None)):
                        callback = functools.partial(
                                self._finish_tracing_callback,
                                handler=handler)
                        result.add_done_callback(callback)
                    else:
                        self._finish_tracing(handler)

                except Exception as exc:
                    self._finish_tracing(handler, error=exc)
                    raise

            return result

        return wrapper

    def _get_operation_name(self, handler):
        full_class_name = type(handler).__name__
        return full_class_name.rsplit('.')[-1]  # package-less name.

    def _finish_tracing_callback(self, future, handler):
        error = future.exception()
        self._finish_tracing(handler, error=error)

    def _apply_tracing(self, handler, attributes):
        """
        Helper function to avoid rewriting for middleware and decorator.
        Returns a new span from the request with logged attributes and
        correct operation name from the func.
        """
        operation_name = self._get_operation_name(handler)
        headers = handler.request.headers
        request = handler.request

        # start new span from trace info
        try:
            span_ctx = self._tracer.extract(opentracing.Format.HTTP_HEADERS,
                                            headers)
            scope = self._tracer.start_active_span(operation_name,
                                                   child_of=span_ctx)
        except (opentracing.InvalidCarrierException,
                opentracing.SpanContextCorruptedException):
            scope = self._tracer.start_active_span(operation_name)

        # add span to current spans
        setattr(request, SCOPE_ATTR, scope)

        # log any traced attributes
        scope.span.set_tag(tags.COMPONENT, 'tornado')
        scope.span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_SERVER)
        scope.span.set_tag(tags.HTTP_METHOD, request.method)
        scope.span.set_tag(tags.HTTP_URL, request.uri)

        for attr in attributes:
            if hasattr(request, attr):
                payload = str(getattr(request, attr))
                if payload:
                    scope.span.set_tag(attr, payload)

        # invoke the start span callback, if any
        self._call_start_span_cb(scope.span, request)

        return scope

    def _finish_tracing(self, handler, error=None):
        scope = getattr(handler.request, SCOPE_ATTR, None)
        if scope is None:
            return

        delattr(handler.request, SCOPE_ATTR)

        if error is not None:
            scope.span.set_tag(tags.ERROR, True)
            scope.span.log_kv({
                'event': tags.ERROR,
                'error.object': error,
            })
        else:
            scope.span.set_tag(tags.HTTP_STATUS_CODE, handler.get_status())

        scope.close()

    def _call_start_span_cb(self, span, request):
        if self._start_span_cb is None:
            return

        try:
            self._start_span_cb(span, request)
        except Exception:
            # TODO - log the error to the Span?
            pass
