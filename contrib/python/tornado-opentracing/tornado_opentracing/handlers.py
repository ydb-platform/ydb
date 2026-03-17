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

from tornado.web import HTTPError

from opentracing.scope_managers.tornado import tracer_stack_context


def execute(func, handler, args, kwargs):
    """
    Wrap the handler ``_execute`` method to trace incoming requests,
    extracting the context from the headers, if available.
    """
    tracing = handler.settings.get('opentracing_tracing')

    with tracer_stack_context():
        if tracing._trace_all:
            attrs = handler.settings.get('opentracing_traced_attributes', [])
            tracing._apply_tracing(handler, attrs)

        return func(*args, **kwargs)


def on_finish(func, handler, args, kwargs):
    """
    Wrap the handler ``on_finish`` method to finish the Span for the
    given request, if available.
    """
    tracing = handler.settings.get('opentracing_tracing')
    tracing._finish_tracing(handler)

    return func(*args, **kwargs)


def log_exception(func, handler, args, kwargs):
    """
    Wrap the handler ``log_exception`` method to finish the Span for the
    given request, if available. This method is called when an Exception
    is not handled in the user code.
    """
    # safe-guard: expected arguments -> log_exception(self, typ, value, tb)
    value = args[1] if len(args) == 3 else None
    if value is None:
        return func(*args, **kwargs)

    tracing = handler.settings.get('opentracing_tracing')
    if not isinstance(value, HTTPError) or 500 <= value.status_code <= 599:
        tracing._finish_tracing(handler, error=value)

    return func(*args, **kwargs)
