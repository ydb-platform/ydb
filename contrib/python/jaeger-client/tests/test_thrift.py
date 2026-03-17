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

from io import BytesIO

import jaeger_client.thrift_gen.jaeger.ttypes as ttypes
import jaeger_client.thrift_gen.sampling.SamplingManager as sampling_manager
from opentracing import child_of, follows_from
from jaeger_client import ProbabilisticSampler, RateLimitingSampler
from jaeger_client import thrift, Span, SpanContext
from jaeger_client.thrift_gen.agent import Agent as Agent
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.transport.TTransport import TMemoryBuffer


def test_submit_batch(tracer):
    span = tracer.start_span('test-span')
    span.set_tag('bender', 'is great')
    span.set_tag('peer.ipv4', 123123)
    span.set_tag('unicode_val', u'non-ascii: \xe9')
    span.set_tag(u'unicode_key_\xe9', 'ascii val')
    span.log_event('kiss-my-shiny-metal-...')
    span.finish()  # to get the duration defined
    # verify that we can serialize the span
    _marshall_span(span)


def _marshall_span(span):
    class TestTrans(TMemoryBuffer):
        def now_reading(self):
            """
            Thrift TMemoryBuffer is not read-able AND write-able,
            it's one or the other (really? yes.). This will convert
            us from write-able to read-able.
            """
            self._buffer = BytesIO(self.getvalue())

    batch = thrift.make_jaeger_batch(
        spans=[span], process=ttypes.Process(serviceName='x', tags={}))

    # write and read them to test encoding
    args = Agent.emitBatch_args(batch)
    t = TestTrans()
    prot = TCompactProtocol(t)
    args.write(prot)
    t.now_reading()
    args.read(prot)


def test_large_ids(tracer):

    def serialize(trace_id, span_id):
        """Checks that there are no exceptions during marshalling."""
        parent_ctx = SpanContext(trace_id=trace_id, span_id=span_id,
                                 parent_id=0, flags=1)
        parent = Span(context=parent_ctx, operation_name='x', tracer=tracer)
        span = tracer.start_span(operation_name='x',
                                 references=child_of(parent.context))
        span.finish()
        _marshall_span(span)

    trace_id = 0x77fd53dc6b437681
    serialize(trace_id, trace_id)
    assert thrift.id_to_int(trace_id) == 0x77fd53dc6b437681

    trace_id = 0x7fffffffffffffff
    serialize(trace_id, trace_id)
    assert thrift.id_to_int(trace_id) == 0x7fffffffffffffff

    trace_id = 0x8000000000000000
    serialize(trace_id, trace_id)
    assert thrift.id_to_int(trace_id) == -0x8000000000000000

    trace_id = 0x97fd53dc6b437681
    serialize(trace_id, trace_id)

    trace_id = (1 << 64) - 1
    assert trace_id == 0xffffffffffffffff
    serialize(trace_id, trace_id)
    assert thrift.id_to_int(trace_id) == -1

    trace_id = (1 << 128) - 1
    span_id = 0xffffffffffffffff
    assert trace_id == 0xffffffffffffffffffffffffffffffff
    serialize(trace_id, span_id)
    assert thrift._id_to_low(trace_id) == 0xffffffffffffffff
    assert thrift._id_to_high(trace_id) == 0xffffffffffffffff

    trace_id = 0xfb34678b8864f051e5c8c603484e57
    span_id = 0x77fd53dc6b437681
    serialize(trace_id, span_id)
    assert thrift._id_to_low(trace_id) == 0x51e5c8c603484e57
    assert thrift._id_to_high(trace_id) == 0xfb34678b8864f0


def test_none_ids():
    assert thrift.id_to_int(None) is None
    assert thrift._id_to_low(None) is None
    assert thrift._id_to_high(None) is None


def test_large_tags():
    tag = thrift.make_tag('x', 'y' * 300, max_length=256,
                          max_traceback_length=512)
    assert len(tag.vStr) <= 256


def test_bool_tags():
    tag = thrift.make_tag('booltag', True, max_length=256,
                          max_traceback_length=512)
    assert tag.vBool is True


def test_bool_tags_false():
    tag = thrift.make_tag('booltag', False, max_length=256,
                          max_traceback_length=512)
    assert tag.vBool is False


def test_long_tags():
    tag = thrift.make_tag('longtag', 404, max_length=256,
                          max_traceback_length=512)
    assert tag.vLong == 404


def test_double_tags():
    tag = thrift.make_tag('doubletag', 12.1, max_length=256,
                          max_traceback_length=512)
    assert tag.vDouble == 12.1


def test_parse_sampling_strategy():
    # probabilistic

    resp = sampling_manager.SamplingStrategyResponse(
        strategyType=sampling_manager.SamplingStrategyType.PROBABILISTIC)
    s, e = thrift.parse_sampling_strategy(response=resp)
    assert s is None and e is not None

    resp.probabilisticSampling = \
        sampling_manager.ProbabilisticSamplingStrategy(samplingRate=2)
    s, e = thrift.parse_sampling_strategy(response=resp)
    assert s is None and e is not None

    resp.probabilisticSampling = \
        sampling_manager.ProbabilisticSamplingStrategy(samplingRate=0.5)
    s, e = thrift.parse_sampling_strategy(response=resp)
    assert isinstance(s, ProbabilisticSampler) and e is None

    # rate limiting

    resp = sampling_manager.SamplingStrategyResponse(
        strategyType=sampling_manager.SamplingStrategyType.RATE_LIMITING)
    s, e = thrift.parse_sampling_strategy(response=resp)
    assert s is None and e is not None

    resp.rateLimitingSampling = \
        sampling_manager.RateLimitingSamplingStrategy(maxTracesPerSecond=-1)
    s, e = thrift.parse_sampling_strategy(response=resp)
    assert s is None and e is not None

    resp.rateLimitingSampling = \
        sampling_manager.RateLimitingSamplingStrategy(maxTracesPerSecond=1)
    s, e = thrift.parse_sampling_strategy(response=resp)
    assert isinstance(s, RateLimitingSampler) and e is None

    # wrong strategy type

    resp.strategyType = 'x'
    s, e = thrift.parse_sampling_strategy(response=resp)
    assert s is None and e is not None


def test_parse_span_references(tracer):
    span = tracer.start_span('test')
    span2 = tracer.start_span('test2')
    follow_span = tracer.start_span('test-follow', references=[follows_from(span.context),
                                                               child_of(span2.context)])
    span.finish()
    span2.finish()
    follow_span.finish()
    _marshall_span(follow_span)
