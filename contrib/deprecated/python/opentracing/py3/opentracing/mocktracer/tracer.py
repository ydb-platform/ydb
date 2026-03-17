# Copyright (c) The OpenTracing Authors.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from threading import Lock
import time

import opentracing
from opentracing import Format, Tracer
from opentracing import UnsupportedFormatException
from opentracing.scope_managers import ThreadLocalScopeManager

from .context import SpanContext
from .span import MockSpan


class MockTracer(Tracer):
    """MockTracer makes it easy to test the semantics of OpenTracing
    instrumentation.

    By using a MockTracer as a :class:`~opentracing.Tracer` implementation
    for tests, a developer can assert that :class:`~opentracing.Span`
    properties and relationships with other
    **Spans** are defined as expected by instrumentation code.

    By default, MockTracer registers propagators for :attr:`Format.TEXT_MAP`,
    :attr:`Format.HTTP_HEADERS` and :attr:`Format.BINARY`. The user should
    call :func:`register_propagator()` for each additional inject/extract
    format.
    """

    def __init__(self, scope_manager=None):
        """Initialize a MockTracer instance."""

        scope_manager = ThreadLocalScopeManager() \
            if scope_manager is None else scope_manager
        super(MockTracer, self).__init__(scope_manager)

        self._propagators = {}
        self._finished_spans = []
        self._spans_lock = Lock()

        # Simple-as-possible (consecutive for repeatability) id generation.
        self._next_id = 0
        self._next_id_lock = Lock()

        self._register_required_propagators()

    def register_propagator(self, format, propagator):
        """Register a propagator with this MockTracer.

        :param string format: a :class:`~opentracing.Format`
            identifier like :attr:`~opentracing.Format.TEXT_MAP`
        :param **Propagator** propagator: a **Propagator** instance to handle
            inject/extract calls involving `format`
        """
        self._propagators[format] = propagator

    def _register_required_propagators(self):
        from .text_propagator import TextPropagator
        from .binary_propagator import BinaryPropagator
        self.register_propagator(Format.TEXT_MAP, TextPropagator())
        self.register_propagator(Format.HTTP_HEADERS, TextPropagator())
        self.register_propagator(Format.BINARY, BinaryPropagator())

    def finished_spans(self):
        """Return a copy of all finished **Spans** started by this MockTracer
        (since construction or the last call to :meth:`~MockTracer.reset()`)

        :rtype: list
        :return: a copy of the finished **Spans**.
        """
        with self._spans_lock:
            return list(self._finished_spans)

    def reset(self):
        """Clear the finished **Spans** queue.

        Note that this does **not** have any effect on **Spans** created by
        MockTracer that have not finished yet; those
        will still be enqueued in :meth:`~MockTracer.finished_spans()`
        when they :func:`finish()`.
        """
        with self._spans_lock:
            self._finished_spans = []

    def _append_finished_span(self, span):
        with self._spans_lock:
            self._finished_spans.append(span)

    def _generate_id(self):
        with self._next_id_lock:
            self._next_id += 1
            return self._next_id

    def start_active_span(self,
                          operation_name,
                          child_of=None,
                          references=None,
                          tags=None,
                          start_time=None,
                          ignore_active_span=False,
                          finish_on_close=True):

        # create a new Span
        span = self.start_span(
            operation_name=operation_name,
            child_of=child_of,
            references=references,
            tags=tags,
            start_time=start_time,
            ignore_active_span=ignore_active_span,
        )

        return self.scope_manager.activate(span, finish_on_close)

    def start_span(self,
                   operation_name=None,
                   child_of=None,
                   references=None,
                   tags=None,
                   start_time=None,
                   ignore_active_span=False):

        start_time = time.time() if start_time is None else start_time

        # See if we have a parent_ctx in `references`
        parent_ctx = None
        if child_of is not None:
            parent_ctx = (
                child_of if isinstance(child_of, opentracing.SpanContext)
                else child_of.context)
        elif references is not None and len(references) > 0:
            # TODO only the first reference is currently used
            parent_ctx = references[0].referenced_context

        # retrieve the active SpanContext
        if not ignore_active_span and parent_ctx is None:
            scope = self.scope_manager.active
            if scope is not None:
                parent_ctx = scope.span.context

        # Assemble the child ctx
        ctx = SpanContext(span_id=self._generate_id())
        if parent_ctx is not None:
            if parent_ctx._baggage is not None:
                ctx._baggage = parent_ctx._baggage.copy()
            ctx.trace_id = parent_ctx.trace_id
        else:
            ctx.trace_id = self._generate_id()

        # Tie it all together
        return MockSpan(
            self,
            operation_name=operation_name,
            context=ctx,
            parent_id=(None if parent_ctx is None else parent_ctx.span_id),
            tags=tags,
            start_time=start_time)

    def inject(self, span_context, format, carrier):
        if format in self._propagators:
            self._propagators[format].inject(span_context, carrier)
        else:
            raise UnsupportedFormatException()

    def extract(self, format, carrier):
        if format in self._propagators:
            return self._propagators[format].extract(carrier)
        else:
            raise UnsupportedFormatException()
