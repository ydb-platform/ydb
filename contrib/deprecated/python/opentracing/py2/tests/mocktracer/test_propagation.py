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

import pytest
from opentracing import Format, SpanContextCorruptedException, \
        UnsupportedFormatException
from opentracing.mocktracer import MockTracer


def test_propagation():
    tracer = MockTracer()
    sp = tracer.start_span(operation_name='test')
    sp.set_baggage_item('foo', 'bar')

    # Test invalid types
    with pytest.raises(UnsupportedFormatException):
        tracer.inject(sp.context, 'invalid', {})
    with pytest.raises(UnsupportedFormatException):
        tracer.extract('invalid', {})

    tests = [(Format.BINARY, bytearray()),
             (Format.TEXT_MAP, {})]
    for format, carrier in tests:
        tracer.inject(sp.context, format, carrier)
        extracted_ctx = tracer.extract(format, carrier)

        assert extracted_ctx.trace_id == sp.context.trace_id
        assert extracted_ctx.span_id == sp.context.span_id
        assert extracted_ctx.baggage == sp.context.baggage


def test_propagation_extract_corrupted_data():
    tracer = MockTracer()

    tests = [(Format.BINARY, bytearray()),
             (Format.TEXT_MAP, {})]
    for format, carrier in tests:
        with pytest.raises(SpanContextCorruptedException):
            tracer.extract(format, carrier)


def test_start_span():
    """ Test in process child span creation."""
    tracer = MockTracer()
    sp = tracer.start_span(operation_name='test')
    sp.set_baggage_item('foo', 'bar')

    child = tracer.start_span(
        operation_name='child', child_of=sp.context)
    assert child.context.trace_id == sp.context.trace_id
    assert child.context.baggage == sp.context.baggage
    assert child.parent_id == sp.context.span_id
