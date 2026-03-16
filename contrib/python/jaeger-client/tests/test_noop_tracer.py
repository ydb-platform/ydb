# Copyright (c) 2016 Uber Technologies, Inc.
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

import opentracing
from opentracing import Tracer, Format


def test_new_trace():
    tracer = Tracer()

    span = tracer.start_span(operation_name='test')
    span.set_baggage_item('Fry', 'Leela')
    span.set_tag('x', 'y')
    span.log_event('z')

    child = tracer.start_span(operation_name='child',
                              references=opentracing.child_of(span.context))
    child.log_event('w')
    assert child.get_baggage_item('Fry') is None
    carrier = {}
    tracer.inject(
        span_context=child.context,
        format=Format.TEXT_MAP,
        carrier=carrier)
    assert carrier == dict()
    child.finish()

    span.finish()


def test_join_trace():
    tracer = Tracer()

    span_ctx = tracer.extract(format=Format.TEXT_MAP, carrier={})
    span = tracer.start_span(operation_name='test',
                             references=opentracing.child_of(span_ctx))
    span.set_tag('x', 'y')
    span.set_baggage_item('a', 'b')
    span.log_event('z')

    child = tracer.start_span(operation_name='child',
                              references=opentracing.child_of(span.context))
    child.log_event('w')
    child.finish()

    span.finish()
