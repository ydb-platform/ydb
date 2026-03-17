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

from opentracing.mocktracer import MockTracer


def test_tracer_finished_spans():
    tracer = MockTracer()

    span_x = tracer.start_span('x')
    span_x.finish()

    span_y = tracer.start_span('y')
    span_y.finish()

    finished_spans = tracer.finished_spans()
    assert len(finished_spans) == 2
    assert finished_spans[0] == span_x
    assert finished_spans[1] == span_y

    # A copy per invocation.
    assert tracer.finished_spans() is not finished_spans


def test_tracer_reset():
    tracer = MockTracer()
    tracer.start_span('x').finish()
    tracer.reset()
    assert len(tracer.finished_spans()) == 0
