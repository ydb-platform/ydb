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

from __future__ import absolute_import
import mock
import time
import types
from opentracing import child_of
from opentracing import Format
from opentracing import Tracer
from opentracing import logs
from opentracing import tags


def test_span():
    tracer = Tracer()
    parent = tracer.start_span('parent')
    child = tracer.start_span('test', references=child_of(parent.context))
    assert parent == child
    child.log_kv({'event': 'cache_hit', 'size.bytes': 42})
    child.log_kv({'event': 'cache_miss'}, time.time())
    child.log_event('cache_hit', ['arg1', 'arg2'])

    with mock.patch.object(parent, 'finish') as finish:
        with mock.patch.object(parent, 'log_event') as log_event:
            with mock.patch.object(parent, 'log_kv') as log_kv:
                with mock.patch.object(parent, 'set_tag') as set_tag:
                    try:
                        with parent:
                            raise ValueError()
                    except ValueError:
                        pass
                    assert finish.call_count == 1
                    assert log_event.call_count == 0
                    assert log_kv.call_count == 1
                    assert set_tag.call_count == 1

    with mock.patch.object(parent, 'finish') as finish:
        with mock.patch.object(parent, 'log_event') as log_kv:
            with parent:
                pass
            assert finish.call_count == 1
            assert log_kv.call_count == 0

    parent.set_tag('x', 'y').set_tag('z', 1)  # test chaining
    parent.set_tag(tags.PEER_SERVICE, 'test-service')
    parent.set_tag(tags.PEER_HOST_IPV4, 127 << 24 + 1)
    parent.set_tag(tags.PEER_HOST_IPV6, '::')
    parent.set_tag(tags.PEER_HOSTNAME, 'uber.com')
    parent.set_tag(tags.PEER_PORT, 123)
    parent.finish()


def test_span_error_report():
    tracer = Tracer()
    span = tracer.start_span('foo')
    error_message = 'unexpected_situation'

    with mock.patch.object(span, 'log_kv') as log_kv:
        with mock.patch.object(span, 'set_tag') as set_tag:
            try:
                with span:
                    raise ValueError(error_message)
            except ValueError:
                pass

            assert set_tag.call_count == 1
            assert set_tag.call_args[0] == (tags.ERROR, True)

            assert log_kv.call_count == 1
            log_kv_args = log_kv.call_args[0][0]
            assert log_kv_args.get(logs.EVENT, None) is tags.ERROR
            assert log_kv_args.get(logs.MESSAGE, None) is error_message
            assert log_kv_args.get(logs.ERROR_KIND, None) is ValueError
            assert isinstance(log_kv_args.get(logs.ERROR_OBJECT, None),
                              ValueError)
            assert isinstance(log_kv_args.get(logs.STACK, None),
                              types.TracebackType)


def test_inject():
    tracer = Tracer()
    span = tracer.start_span()

    bin_carrier = bytearray()
    tracer.inject(
        span_context=span.context,
        format=Format.BINARY,
        carrier=bin_carrier)
    assert bin_carrier == bytearray()

    text_carrier = {}
    tracer.inject(
        span_context=span.context,
        format=Format.TEXT_MAP,
        carrier=text_carrier)
    assert text_carrier == {}


def test_extract():
    tracer = Tracer()
    noop_span = tracer._noop_span

    bin_carrier = bytearray()
    span_ctx = tracer.extract(Format.BINARY, carrier=bin_carrier)
    assert noop_span.context == span_ctx

    text_carrier = {}
    span_ctx = tracer.extract(Format.TEXT_MAP, carrier=text_carrier)
    assert noop_span.context == span_ctx
