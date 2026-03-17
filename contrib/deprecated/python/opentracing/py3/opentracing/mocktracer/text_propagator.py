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

from __future__ import absolute_import

from opentracing import SpanContextCorruptedException

from .context import SpanContext
from .propagator import Propagator

prefix_tracer_state = 'ot-tracer-'
prefix_baggage = 'ot-baggage-'
field_name_trace_id = prefix_tracer_state + 'traceid'
field_name_span_id = prefix_tracer_state + 'spanid'
field_count = 2


class TextPropagator(Propagator):
    """A MockTracer Propagator for Format.TEXT_MAP."""

    def inject(self, span_context, carrier):
        carrier[field_name_trace_id] = '{0:x}'.format(span_context.trace_id)
        carrier[field_name_span_id] = '{0:x}'.format(span_context.span_id)
        if span_context.baggage is not None:
            for k in span_context.baggage:
                carrier[prefix_baggage+k] = span_context.baggage[k]

    def extract(self, carrier):  # noqa
        count = 0
        span_id, trace_id = (0, 0)
        baggage = {}
        for k in carrier:
            v = carrier[k]
            k = k.lower()
            if k == field_name_span_id:
                span_id = int(v, 16)
                count += 1
            elif k == field_name_trace_id:
                trace_id = int(v, 16)
                count += 1
            elif k.startswith(prefix_baggage):
                baggage[k[len(prefix_baggage):]] = v

        if count != field_count:
            raise SpanContextCorruptedException()

        return SpanContext(
            span_id=span_id,
            trace_id=trace_id,
            baggage=baggage)
