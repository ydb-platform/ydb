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

import mock
import random
import socket

import pytest
import tornado.httputil

from opentracing import Format, child_of, follows_from
from opentracing.ext import tags as ext_tags
from jaeger_client import ConstSampler, SpanContext, Tracer
from jaeger_client import constants as c


def find_tag(span, key, tag_type='str'):
    for tag in span.tags:
        if tag.key == key:
            if tag_type == 'str':
                return tag.vStr
            elif tag_type == 'bool':
                return tag.vBool
    return None


def test_start_trace(tracer):
    assert type(tracer) is Tracer
    with mock.patch.object(random.Random, 'getrandbits') as mock_random, \
            mock.patch('time.time') as mock_timestamp:
        mock_random.return_value = 12345
        mock_timestamp.return_value = 54321

        span = tracer.start_span('test')
        span.set_tag(ext_tags.SPAN_KIND, ext_tags.SPAN_KIND_RPC_SERVER)
        assert span, 'Span must not be nil'
        assert span.tracer == tracer, 'Tracer must be referenced from span'
        assert find_tag(span, 'span.kind') == ext_tags.SPAN_KIND_RPC_SERVER, \
            'Span must be server-side'
        assert span.trace_id == 12345, 'Must match trace_id'
        assert span.is_sampled(), 'Must be sampled'
        assert span.parent_id is None, 'Must not have parent_id (root span)'
        assert span.start_time == 54321, 'Must match timestamp'

        span.finish()
        assert span.end_time is not None, 'Must have end_time defined'
        tracer.reporter.report_span.assert_called_once()

    tracer.close()


def test_forced_sampling(tracer):
    tracer.sampler = ConstSampler(False)
    span = tracer.start_span('test2',
                             tags={ext_tags.SAMPLING_PRIORITY: 1})
    assert span.is_sampled()
    assert span.is_debug()


@pytest.mark.parametrize('mode,', ['arg', 'ref'])
def test_start_child(tracer, mode):
    root = tracer.start_span('test')
    if mode == 'arg':
        span = tracer.start_span('test', child_of=root.context)
    elif mode == 'ref':
        span = tracer.start_span('test', references=child_of(root.context))
    else:
        raise ValueError('bad mode')
    span.set_tag(ext_tags.SPAN_KIND, ext_tags.SPAN_KIND_RPC_SERVER)
    assert span.is_sampled(), 'Must be sampled'
    assert span.trace_id == root.trace_id, 'Must have the same trace id'
    assert span.parent_id == root.span_id, 'Must inherit parent id'
    span.finish()
    assert span.end_time is not None, 'Must have end_time set'
    tracer.reporter.report_span.assert_called_once()
    tracer.close()


@pytest.mark.parametrize('one_span_per_rpc,', [True, False])
def test_one_span_per_rpc(tracer, one_span_per_rpc):
    tracer.one_span_per_rpc = one_span_per_rpc
    span = tracer.start_span('client')
    span.set_tag(ext_tags.SPAN_KIND, ext_tags.SPAN_KIND_RPC_CLIENT)
    child = tracer.start_span(
        'server',
        references=child_of(span.context),
        tags={ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_RPC_SERVER},
    )
    assert span.trace_id == child.trace_id, 'Must have the same trace ids'
    if one_span_per_rpc:
        assert span.span_id == child.span_id, 'Must have the same span ids'
    else:
        assert span.span_id != child.span_id, 'Must have different span ids'


def test_child_span(tracer):
    span = tracer.start_span('test')
    child = tracer.start_span('child', references=child_of(span.context))
    child.set_tag(ext_tags.SPAN_KIND, ext_tags.SPAN_KIND_RPC_CLIENT)
    child.set_tag('bender', 'is great')
    child.log_event('kiss-my-shiny-metal-...')
    child.finish()
    span.finish()
    tracer.reporter.report_span.assert_called()
    assert len(span.logs) == 0, 'Parent span is Local, must not have events'
    assert len(child.logs) == 1, 'Child must have one events'

    tracer.sampler = ConstSampler(False)
    span = tracer.start_span('test')
    child = tracer.start_span('child', references=child_of(span.context))
    child.set_tag('bender', 'is great')
    child.log_event('kiss-my-shiny-metal-...')
    child.finish()
    span.finish()
    assert len(child.logs) == 0, 'Must have no events, not sampled'
    assert len(child.tags) == 0, 'Must have no attributes, not sampled'
    tracer.close()


def test_follows_from(tracer):
    span = tracer.start_span('test')
    span1 = tracer.start_span('test2')
    follow_span = tracer.start_span('follow-span', references=[follows_from(span.context),
                                                               follows_from(span1.context),
                                                               follows_from(None)])
    span.finish()
    span1.finish()
    follow_span.finish()
    tracer.reporter.report_span.assert_called()
    assert len(follow_span.references) == 2
    assert follow_span.context.parent_id == span.context.span_id
    for reference in follow_span.references:
        assert reference.referenced_context is not None

    tracer.reporter = mock.MagicMock()
    span = tracer.start_span('test')
    follow_span = tracer.start_span(references=follows_from(span.context))
    span.finish()
    follow_span.finish()
    tracer.reporter.report_span.assert_called()
    assert isinstance(follow_span.references, list)

    tracer.reporter = mock.MagicMock()
    span = tracer.start_span('test')
    parent_span = tracer.start_span('test-parent')
    child_span = tracer.start_span('test-child', child_of=parent_span,
                                   references=follows_from(span.context))
    span.finish()
    parent_span.finish()
    child_span.finish()
    tracer.reporter.report_span.assert_called()
    assert child_span.context.parent_id == parent_span.context.span_id
    assert len(child_span.references) == 1
    tracer.close()


def test_sampler_effects(tracer):
    tracer.sampler = ConstSampler(True)
    span = tracer.start_span('test')
    assert span.is_sampled(), 'Must be sampled'

    tracer.sampler = ConstSampler(False)
    span = tracer.start_span('test')
    assert not span.is_sampled(), 'Must not be sampled'
    tracer.close()


@pytest.mark.parametrize('inject_mode', ['span', 'context'])
def test_serialization(tracer, inject_mode):
    span = tracer.start_span('help')
    carrier = {}
    if inject_mode == 'span':
        injectable = span
    elif inject_mode == 'context':
        injectable = span.context
    else:
        raise ValueError('bad inject_mode')
    tracer.inject(
        span_context=injectable, format=Format.TEXT_MAP, carrier=carrier
    )
    assert len(carrier) > 0
    h_ctx = tornado.httputil.HTTPHeaders(carrier)
    assert 'UBER-TRACE-ID' in h_ctx
    ctx2 = tracer.extract(Format.TEXT_MAP, carrier)
    assert ctx2 is not None
    assert ctx2.trace_id == span.trace_id
    assert ctx2.span_id == span.span_id
    assert ctx2.parent_id == span.parent_id
    assert ctx2.flags == span.flags


def test_serialization_error(tracer):
    span = 'span'
    carrier = {}
    with pytest.raises(ValueError):
        tracer.inject(
            span_context=span, format=Format.TEXT_MAP, carrier=carrier
        )


def test_tracer_tags():
    reporter = mock.MagicMock()
    sampler = ConstSampler(True)

    with mock.patch('socket.gethostname', return_value='dream-host.com'):
        t = Tracer(service_name='x', reporter=reporter, sampler=sampler)
        assert t.tags.get('hostname') == 'dream-host.com'
        assert 'ip' in t.tags
        assert 'jaeger.version' in t.tags


def test_tracer_tags_passed_to_reporter():
    reporter = mock.MagicMock()
    reporter.set_process = mock.MagicMock()
    sampler = ConstSampler(True)
    tracer = Tracer(
        service_name='x', reporter=reporter, sampler=sampler,
        max_tag_value_length=123,
    )
    reporter.set_process.assert_called_once_with(
        service_name='x', tags=tracer.tags, max_length=123,
    )


def test_tracer_tags_no_hostname():
    reporter = mock.MagicMock()
    sampler = ConstSampler(True)

    from jaeger_client.tracer import logger
    with mock.patch.object(logger, 'exception') as mock_log:
        with mock.patch('socket.gethostname',
                        side_effect=['host', socket.timeout()]):
            Tracer(service_name='x', reporter=reporter, sampler=sampler)
        assert mock_log.call_count == 1


@pytest.mark.parametrize('span_type,expected_tags', [
    ('root', {
        'sampler.type': 'const',
        'sampler.param': True,
    }),
    ('child', {
        'sampler.type': None,
        'sampler.param': None,
    }),
    ('rpc-server', {
        'sampler.type': None,
        'sampler.param': None,
    }),
])
def test_tracer_tags_on_root_span(span_type, expected_tags):
    reporter = mock.MagicMock()
    sampler = ConstSampler(True)
    with mock.patch('socket.gethostname', return_value='dream-host.com'):
        tracer = Tracer(service_name='x',
                        reporter=reporter,
                        sampler=sampler,
                        tags={'global-tag': 'global-tag'})
        span = tracer.start_span(operation_name='root')
        if span_type == 'child':
            span = tracer.start_span('child', child_of=span)
        if span_type == 'rpc-server':
            span = tracer.start_span(
                'child', child_of=span.context,
                tags={ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_RPC_SERVER}
            )
        for key, value in expected_tags.items():
            found_tag = find_tag(span, key, type(value).__name__)
            if value is None:
                assert found_tag is None, 'test (%s)' % span_type
                continue

            assert found_tag == value, \
                'test (%s): expecting tag %s=%s' % (span_type, key, value)


def test_tracer_override_codecs():
    reporter = mock.MagicMock()
    sampler = ConstSampler(True)
    codecs = {
        'extra_codec': 'codec_placeholder',
        Format.BINARY: 'overridden_binary_codec'

    }
    with mock.patch('socket.gethostname', return_value='dream-host.com'):
        tracer = Tracer(service_name='x', reporter=reporter, sampler=sampler,
                        extra_codecs=codecs)
        assert tracer.codecs['extra_codec'] == 'codec_placeholder',\
                                               'Extra codec not found'
        assert tracer.codecs[Format.BINARY] == 'overridden_binary_codec',\
                                               'Binary format codec not overridden'


def test_tracer_hostname_tag():
    reporter = mock.MagicMock()
    sampler = ConstSampler(True)
    tracer = Tracer(
        service_name='x',
        tags={c.JAEGER_HOSTNAME_TAG_KEY: 'jaeger-client-app.local'},
        reporter=reporter,
        sampler=sampler,
    )

    assert tracer.tags[c.JAEGER_HOSTNAME_TAG_KEY] == 'jaeger-client-app.local'


def test_tracer_ip_tag():
    reporter = mock.MagicMock()
    sampler = ConstSampler(True)
    tracer = Tracer(
        service_name='x',
        tags={c.JAEGER_IP_TAG_KEY: '192.0.2.3'},
        reporter=reporter,
        sampler=sampler,
    )

    assert tracer.tags[c.JAEGER_IP_TAG_KEY] == '192.0.2.3'


def test_tracer_throttler():
    tracer = Tracer(
        service_name='x',
        reporter=mock.MagicMock(),
        sampler=mock.MagicMock(),
        throttler=mock.MagicMock())
    tracer.throttler.is_allowed.return_value = True
    assert tracer.is_debug_allowed()
    tracer.throttler.is_allowed.return_value = False
    assert not tracer.is_debug_allowed()
    debug_span_context = SpanContext.with_debug_id('debug-id')
    span = tracer.start_span('test-operation', child_of=debug_span_context)
    assert not span.is_debug()


def test_tracer_128bit_trace_id():
    reporter = mock.MagicMock()
    sampler = mock.MagicMock()
    tracer = Tracer(
        service_name='x',
        reporter=reporter,
        sampler=sampler,
    )
    assert tracer.max_trace_id_bits == c._max_id_bits

    tracer = Tracer(
        service_name='x',
        reporter=reporter,
        sampler=sampler,
        generate_128bit_trace_id=True,
    )
    assert tracer.max_trace_id_bits == c._max_trace_id_bits
