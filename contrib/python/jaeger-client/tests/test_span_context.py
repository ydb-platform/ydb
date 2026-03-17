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


from jaeger_client import SpanContext


def test_parent_id_to_none():
    ctx1 = SpanContext(trace_id=1, span_id=2, parent_id=0, flags=1)
    assert ctx1.parent_id is None


def test_with_baggage_items():
    baggage1 = {'x': 'y'}
    ctx1 = SpanContext(trace_id=1, span_id=2, parent_id=3, flags=1,
                       baggage=baggage1)
    ctx2 = ctx1.with_baggage_item('a', 'b')
    assert ctx1.trace_id == ctx2.trace_id
    assert ctx1.span_id == ctx2.span_id
    assert ctx1.parent_id == ctx2.parent_id
    assert ctx1.flags == ctx2.flags
    assert ctx1.baggage != ctx2.baggage
    baggage1['a'] = 'b'
    assert ctx1.baggage == ctx2.baggage

    ctx3 = ctx2.with_baggage_item('a', None)
    assert ctx2.baggage != ctx3.baggage
    baggage1.pop('a')
    assert ctx3.baggage == baggage1


def test_is_debug_id_container_only():
    ctx = SpanContext.with_debug_id('value1')
    assert ctx.is_debug_id_container_only
    assert ctx.debug_id == 'value1'
    ctx = SpanContext(trace_id=1, span_id=2, parent_id=3, flags=1)
    assert not ctx.is_debug_id_container_only
