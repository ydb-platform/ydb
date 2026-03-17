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

import opentracing


class SpanContext(opentracing.SpanContext):
    """SpanContext satisfies the opentracing.SpanContext contract.

    trace_id and span_id are uint64's, so their range is [1, 2^64).
    """

    def __init__(
            self,
            trace_id=None,
            span_id=None,
            baggage=None):
        self.trace_id = trace_id
        self.span_id = span_id
        self._baggage = baggage or opentracing.SpanContext.EMPTY_BAGGAGE

    @property
    def baggage(self):
        return self._baggage

    def with_baggage_item(self, key, value):
        new_baggage = self._baggage.copy()
        new_baggage[key] = value
        return SpanContext(
            trace_id=self.trace_id,
            span_id=self.span_id,
            baggage=new_baggage)
