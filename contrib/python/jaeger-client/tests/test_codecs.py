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


import unittest
from collections import namedtuple
from itertools import product

import mock
import pytest
from jaeger_client import Span, SpanContext, Tracer, ConstSampler
from jaeger_client.codecs import (
    Codec, TextCodec, BinaryCodec, ZipkinCodec, ZipkinSpanFormat, B3Codec,
    span_context_from_string,
    span_context_to_string,
)
from jaeger_client.config import Config
from jaeger_client.reporter import InMemoryReporter
from jaeger_client import constants
from opentracing import Format
from opentracing.propagation import (
    InvalidCarrierException,
    SpanContextCorruptedException,
)


class TestCodecs(unittest.TestCase):

    def test_abstract_codec(self):
        codec = Codec()
        with self.assertRaises(NotImplementedError):
            codec.inject({}, {})
        with self.assertRaises(NotImplementedError):
            codec.extract({})

    def test_wrong_carrier(self):
        codec = TextCodec()
        with self.assertRaises(InvalidCarrierException):
            codec.inject(span_context={}, carrier=[])  # array is no good
        with self.assertRaises(InvalidCarrierException):
            codec.extract(carrier=[])

    def test_trace_context_from_bad_string(self):
        tests = [
            (123.321, 'not a string'),
            ('bad value', 'bad string'),
            ('1:1:1:1:1', 'Too many colons'),
            ('1:1:1', 'Too few colons'),
            ('x:1:1:1', 'Not all numbers'),
            ('1:x:1:1', 'Not all numbers'),
            ('1:1:x:1', 'Not all numbers'),
            ('1:1:1:x', 'Not all numbers'),
            ('0:1:1:1', 'Trace ID cannot be zero'),
            ('1:0:1:1', 'Span ID cannot be zero'),
            ('1:1:-1:1', 'Parent ID cannot be negative'),
            ('1:1::1', 'Parent ID is missing'),
            ('1:1:1:-1', 'Flags cannot be negative'),
        ]

        for test in tests:
            try:
                val = span_context_from_string(test[0])
            except SpanContextCorruptedException:
                val = None
            self.assertEqual(val, None, test[1])

    def test_trace_context_from_to_string(self):
        to_string = span_context_to_string
        from_string = span_context_from_string

        tests = [
            [(256, 127, None, 1), '100:7f:0:1'],
            [(256, 127, 256, 0), '100:7f:100:0'],
            [(0xffffffffffffffffffffffffffffffff, 127, 256, 0),
             'ffffffffffffffffffffffffffffffff:7f:100:0'],
        ]
        for test in tests:
            ctx = test[0]
            value = to_string(*ctx)
            self.assertEqual(value, test[1])
            ctx_rev = from_string(value)
            self.assertEqual(ctx_rev, ctx)

        ctx_rev = from_string(['100:7f:100:0'])
        assert ctx_rev == (256, 127, 256, 0), 'Array is acceptable'

        with self.assertRaises(SpanContextCorruptedException):
            from_string(['100:7f:100:0', 'garbage'])

        ctx_rev = from_string(u'100:7f:100:0')
        assert ctx_rev == (256, 127, 256, 0), 'Unicode is acceptable'

    def test_context_to_readable_headers(self):
        for url_encoding in [False, True]:
            codec = TextCodec(
                url_encoding=url_encoding,
                trace_id_header='Trace_ID',
                baggage_header_prefix='Trace-Attr-')
            ctx = SpanContext(
                trace_id=256, span_id=127, parent_id=None, flags=1
            )
            carrier = {}
            codec.inject(ctx, carrier)
            assert carrier == {'trace-id': '100:7f:0:1'}

            ctx._baggage = {
                'bender': 'Countess de la Roca',
                'fry': u'Leela',
                b'key1': bytes([75]),
                u'key2': 'cafe',
                u'key3': u'\U0001F47E',
            }
            carrier = {}
            codec.inject(ctx, carrier)
            # NB: the reverse transformation is not exact, e.g. this fails:
            #   assert ctx._baggage == codec.extract(carrier)._baggage
            # But fully supporting lossless Unicode baggage is not the goal.
            if url_encoding:
                assert carrier == {
                    'trace-id': '100:7f:0:1',
                    'trace-attr-bender': 'Countess%20de%20la%20Roca',
                    'trace-attr-fry': 'Leela',
                    'trace-attr-key1': 'K',
                    'trace-attr-key2': 'cafe',
                    'trace-attr-key3': '%F0%9F%91%BE',
                }, 'with url_encoding = %s' % url_encoding
                for key, val in carrier.items():
                    assert isinstance(key, str)
                    assert isinstance(val, str), '%s' % type(val)
            else:
                assert carrier == {
                    'trace-id': '100:7f:0:1',
                    'trace-attr-bender': 'Countess de la Roca',
                    'trace-attr-fry': 'Leela',
                    'trace-attr-key1': 'K',
                    u'trace-attr-key2': 'cafe',
                    'trace-attr-key3': u'\U0001F47E',
                }, 'with url_encoding = %s' % url_encoding

    def test_context_from_bad_readable_headers(self):
        codec = TextCodec(trace_id_header='Trace_ID',
                          baggage_header_prefix='Trace-Attr-')

        ctx = codec.extract(dict())
        assert ctx is None, 'No headers'

        bad_headers = {
            '_Trace_ID': '100:7f:0:1',
            '_trace-attr-Kiff': 'Amy'
        }
        ctx = codec.extract(bad_headers)
        assert ctx is None, 'Bad header names'

        with self.assertRaises(InvalidCarrierException):
            codec.extract(carrier=[])  # not a dict

        good_headers_bad_values = {
            'Trace-ID': '100:7f:0:1xxx',
            'trace-attr-Kiff': 'Amy'
        }
        with self.assertRaises(SpanContextCorruptedException):
            codec.extract(good_headers_bad_values)

    def test_context_from_readable_headers(self):
        # provide headers all the way through Config object
        config = Config(
            service_name='test',
            config={
                'trace_id_header': 'Trace_ID',
                'baggage_header_prefix': 'Trace-Attr-',
            })
        tracer = config.create_tracer(
            reporter=InMemoryReporter(),
            sampler=ConstSampler(True),
        )
        for url_encoding in [False, True]:
            if url_encoding:
                codec = tracer.codecs[Format.HTTP_HEADERS]
                headers = {
                    'Trace-ID': '100%3A7f:0:1',
                    'trace-attr-Kiff': 'Amy%20Wang',
                    'trace-atTR-HERMES': 'LaBarbara%20Hermes'
                }
            else:
                codec = tracer.codecs[Format.HTTP_HEADERS]
                headers = {
                    'Trace-ID': '100:7f:0:1',
                    'trace-attr-Kiff': 'Amy Wang',
                    'trace-atTR-HERMES': 'LaBarbara Hermes'
                }
            ctx = codec.extract(headers)
            assert ctx.trace_id == 256
            assert ctx.span_id == 127
            assert ctx.parent_id is None
            assert ctx.flags == 1
            assert ctx.baggage == {
                'kiff': 'Amy Wang',
                'hermes': 'LaBarbara Hermes',
            }

    def test_context_from_large_ids(self):
        codec = TextCodec(trace_id_header='Trace_ID',
                          baggage_header_prefix='Trace-Attr-')
        headers = {
            'Trace-ID': 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFF:FFFFFFFFFFFFFFFF:1',
        }
        context = codec.extract(headers)
        assert context.trace_id == 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
        assert context.trace_id == (1 << 128) - 1
        assert context.trace_id > 0
        assert context.span_id == 0xFFFFFFFFFFFFFFFF
        assert context.span_id == (1 << 64) - 1
        assert context.span_id > 0
        assert context.parent_id == 0xFFFFFFFFFFFFFFFF
        assert context.parent_id == (1 << 64) - 1
        assert context.parent_id > 0

    def test_zipkin_codec_extract(self):
        codec = ZipkinCodec()

        t = namedtuple('Tracing', 'span_id parent_id trace_id traceflags')
        carrier = t(span_id=1, parent_id=2, trace_id=3, traceflags=1)
        context = codec.extract(carrier)
        assert 3 == context.trace_id
        assert 2 == context.parent_id
        assert 1 == context.span_id
        assert 1 == context.flags
        assert context.baggage == {}

        t = namedtuple('Tracing', 'something')
        carrier = t(something=1)
        with self.assertRaises(InvalidCarrierException):
            codec.extract(carrier)

        t = namedtuple('Tracing', 'trace_id')
        carrier = t(trace_id=1)
        with self.assertRaises(InvalidCarrierException):
            codec.extract(carrier)

        t = namedtuple('Tracing', 'trace_id span_id')
        carrier = t(trace_id=1, span_id=1)
        with self.assertRaises(InvalidCarrierException):
            codec.extract(carrier)

        t = namedtuple('Tracing', 'trace_id span_id parent_id')
        carrier = t(trace_id=1, span_id=1, parent_id=1)
        with self.assertRaises(InvalidCarrierException):
            codec.extract(carrier)

        carrier = {'span_id': 1, 'parent_id': 2, 'trace_id': 3,
                   'traceflags': 1}
        context = codec.extract(carrier)
        assert 3 == context.trace_id
        assert 2 == context.parent_id
        assert 1 == context.span_id
        assert 1 == context.flags
        assert context.baggage == {}

        carrier['trace_id'] = 0
        assert codec.extract(carrier) is None

    def test_zipkin_codec_inject(self):
        codec = ZipkinCodec()

        with self.assertRaises(InvalidCarrierException):
            codec.inject(span_context=None, carrier=[])

        ctx = SpanContext(trace_id=256, span_id=127, parent_id=None, flags=1)
        span = Span(context=ctx, operation_name='x', tracer=None, start_time=1)
        carrier = {}
        codec.inject(span_context=span, carrier=carrier)
        assert carrier == {'span_id': 127, 'parent_id': None,
                           'trace_id': 256, 'traceflags': 1}

    def test_zipkin_b3_codec_inject(self):
        codec = B3Codec()

        with self.assertRaises(InvalidCarrierException):
            codec.inject(span_context=None, carrier=[])

        ctx = SpanContext(trace_id=256, span_id=127, parent_id=None, flags=2)
        span = Span(context=ctx, operation_name='x', tracer=None, start_time=1)
        carrier = {}
        codec.inject(span_context=span, carrier=carrier)
        assert carrier == {'X-B3-SpanId': format(127, 'x').zfill(16),
                           'X-B3-TraceId': format(256, 'x').zfill(16), 'X-B3-Flags': '1'}

    def test_b3_codec_inject_parent(self):
        codec = B3Codec()

        with self.assertRaises(InvalidCarrierException):
            codec.inject(span_context=None, carrier=[])

        ctx = SpanContext(trace_id=256, span_id=127, parent_id=32, flags=1)
        span = Span(context=ctx, operation_name='x', tracer=None, start_time=1)
        carrier = {}
        codec.inject(span_context=span, carrier=carrier)
        assert carrier == {'X-B3-SpanId': format(127, 'x').zfill(16),
                           'X-B3-ParentSpanId': format(32, 'x').zfill(16),
                           'X-B3-TraceId': format(256, 'x').zfill(16), 'X-B3-Sampled': '1'}

    def test_b3_extract(self):
        codec = B3Codec()

        with self.assertRaises(InvalidCarrierException):
            codec.extract([])

        # Implicit case insensitivity testing
        carrier = {'X-b3-SpanId': 'a2fb4a1d1a96d312', 'X-B3-ParentSpanId': '0020000000000001',
                   'X-B3-traceId': '463ac35c9f6413ad48485a3953bb6124', 'X-B3-flags': '1'}

        span_context = codec.extract(carrier)
        assert span_context.span_id == int('a2fb4a1d1a96d312', 16)
        assert span_context.trace_id == int('463ac35c9f6413ad48485a3953bb6124', 16)
        assert span_context.parent_id == int('0020000000000001', 16)
        assert span_context.flags == 0x02

        # validate that missing parentspanid does not cause an error
        carrier.pop('X-B3-ParentSpanId')
        span_context = codec.extract(carrier)
        assert span_context.parent_id is None

        carrier.update({'X-b3-sampled': '1'})

        span_context = codec.extract(carrier)
        assert span_context.flags == 0x03

        carrier.pop('X-B3-flags')
        span_context = codec.extract(carrier)
        assert span_context.flags == 0x01

        # validate present debug header with falsy value
        carrier = {'X-b3-SpanId': 'a2fb4a1d1a96d312', 'X-B3-flags': '0',
                   'X-B3-traceId': '463ac35c9f6413ad48485a3953bb6124'}
        span_context = codec.extract(carrier)
        assert span_context.flags == 0x00

        # validate missing context
        assert codec.extract({}) is None

        # validate explicit none in context
        carrier = {'X-b3-SpanId': None,
                   'X-B3-traceId': '463ac35c9f6413ad48485a3953bb6124'}
        assert codec.extract(carrier) is None

        # validate invalid hex string
        with self.assertRaises(SpanContextCorruptedException):
            codec.extract({'x-B3-TraceId': 'a2fb4a1d1a96d312z'})

        # validate non-string header
        with self.assertRaises(SpanContextCorruptedException):
            codec.extract({'x-B3-traceId': 123})

    def test_zipkin_b3_codec_extract_injected(self):
        codec = B3Codec()
        ctx = SpanContext(trace_id=256, span_id=127, parent_id=None, flags=0)
        span = Span(context=ctx, operation_name='x', tracer=None, start_time=1)
        carrier = {}
        codec.inject(span_context=span, carrier=carrier)

        extracted = codec.extract(carrier)
        assert extracted.trace_id == ctx.trace_id
        assert extracted.span_id == ctx.span_id
        assert extracted.parent_id == ctx.parent_id
        assert extracted.flags == ctx.flags

    def test_128bit_trace_id_with_zero_padding(self):
        codec = B3Codec(generate_128bit_trace_id=True)

        carrier_1 = {'X-B3-SpanId': '39fe73de0012a0e5', 'X-B3-ParentSpanId': '3dbf8a511e159b05',
                     'X-B3-TraceId': '023f352eaefd8b887a06732f5312e2de', 'X-B3-Flags': '0'}
        span_context = codec.extract(carrier_1)

        carrier_2 = {}
        codec.inject(span_context=span_context, carrier=carrier_2)
        assert carrier_1['X-B3-TraceId'] == carrier_2['X-B3-TraceId']

    def test_binary_codec(self):
        codec = BinaryCodec()
        with self.assertRaises(InvalidCarrierException):
            codec.inject({}, {})
        with self.assertRaises(InvalidCarrierException):
            codec.extract({})

        tracer = Tracer(
            service_name='test',
            reporter=InMemoryReporter(),
            sampler=ConstSampler(True),
        )
        baggage = {'baggage_1': u'data',
                   u'baggage_2': 'foobar',
                   'baggage_3': '\x00\x01\x09\xff',
                   u'baggage_4': u'\U0001F47E'}

        span_context = SpanContext(trace_id=260817200211625699950706086749966912306, span_id=567890,
                                   parent_id=1234567890, flags=1,
                                   baggage=baggage)

        carrier = bytearray()
        tracer.inject(span_context, Format.BINARY, carrier)
        assert len(carrier) != 0

        extracted_span_context = tracer.extract(Format.BINARY, carrier)
        assert extracted_span_context.trace_id == span_context.trace_id
        assert extracted_span_context.span_id == span_context.span_id
        assert extracted_span_context.parent_id == span_context.parent_id
        assert extracted_span_context.flags == span_context.flags
        assert extracted_span_context.baggage == span_context.baggage

    def test_binary_codec_extract_compatibility_with_golang_client(self):
        tracer = Tracer(
            service_name='test',
            reporter=InMemoryReporter(),
            sampler=ConstSampler(True),
        )
        tests = {
            b'\x00\x00\x00\x00\x00\x00\x00\x00u\x18\xa9\x13\xa0\xd2\xaf4u\x18\xa9\x13\xa0\xd2\xaf4'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00':
                {'trace_id_high': 0,
                 'trace_id_low': 8437679803646258996,
                 'span_id': 8437679803646258996,
                 'parent_id': None,
                 'flags': 1,
                 'baggage_count': 0,
                 'baggage': {}},
            b'K2\x88\x8b\x8f\xb5\x96\xe9+\xc6\xe6\xf5\x9d\xed\x8a\xd0+\xc6\xe6\xf5\x9d\xed\x8a\xd0'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00':
                {'trace_id_high': 5418543434673002217,
                 'trace_id_low': 3154462531610577616,
                 'span_id': 3154462531610577616,
                 'parent_id': None,
                 'flags': 1,
                 'baggage_count': 0,
                 'baggage': {}},
            b'd\xb7^Y\x1afI\x0bi\xe4lc`\x1e\xbep[\x0fw\xc8\x87\xfd\xb2Ti\xe4lc`\x1e\xbep\x01\x00'
            b'\x00\x00\x00':
                {'trace_id_high': 7257373061318854923,
                 'trace_id_low': 7630342842742652528,
                 'span_id': 6561594885260816980,
                 'parent_id': 7630342842742652528,
                 'flags': 1,
                 'baggage_count': 0,
                 'baggage': {}},
            b'a]\x85\xe0\xe0\x06\xd5[6k\x9d\x86\xaa\xbc\\\x8f#c\x06\x80jV\xdf\x826k\x9d\x86\xaa\xbc'
            b'\\\x8f\x01\x00\x00\x00\x01\x00\x00\x00\x07key_one\x00\x00\x00\tvalue_one':
                {'trace_id_high': 7015910995390813531,
                 'trace_id_low': 3921401102271798415,
                 'span_id': 2549888962631491458,
                 'parent_id': 3921401102271798415,
                 'flags': 1,
                 'baggage_count': 1,
                 'baggage': {'key_one': 'value_one'}
                 },
        }

        for span_context_serialized, expected in tests.items():
            span_context = tracer.extract(Format.BINARY, bytearray(span_context_serialized))
            # because python supports 128bit number as one number and go splits it in two 64 bit
            # numbers, we need to split python number to compare it properly to go implementation
            max_int64 = 0xFFFFFFFFFFFFFFFF
            trace_id_high = (span_context.trace_id >> 64) & max_int64
            trace_id_low = span_context.trace_id & max_int64

            assert trace_id_high == expected['trace_id_high']
            assert trace_id_low == expected['trace_id_low']
            assert span_context.span_id == expected['span_id']
            assert span_context.parent_id == expected['parent_id']
            assert span_context.flags == expected['flags']
            assert len(span_context.baggage) == expected['baggage_count']
            assert span_context.baggage == expected['baggage']

            carrier = bytearray()
            tracer.inject(span_context, Format.BINARY, carrier)
            assert carrier == bytearray(span_context_serialized)


def test_default_baggage_without_trace_id(tracer):
    _test_baggage_without_trace_id(
        tracer=tracer,
        trace_id_header='Trace_ID',
        baggage_header_prefix='Trace-baggage-',
        headers={
            'Trace-ID': '1:7f:0:1',
            'trace-baggage-Kiff': 'Amy',
            'trace-BAGGAGE-HERMES': 'LaBarbara',
        },
        match={
            'kiff': 'Amy',
            'hermes': 'LaBarbara',
        },
    )


def test_ad_hoc_baggage_without_trace_id(tracer):
    _test_baggage_without_trace_id(
        tracer=tracer,
        trace_id_header='Trace_ID',
        baggage_header_prefix='Trace-baggage-',
        headers={
            'Trace-ID': '1:7f:0:1',
            'jaeger-baggage': 'kiff=Amy, hermes=LaBarbara, bender=Bender',
        },
        match={
            'kiff': 'Amy',
            'hermes': 'LaBarbara',
            'bender': 'Bender',
        },
    )


def _test_baggage_without_trace_id(tracer, trace_id_header, baggage_header_prefix, headers, match):
    codec = TextCodec(
        trace_id_header=trace_id_header,
        baggage_header_prefix=baggage_header_prefix,
    )
    with mock.patch('jaeger_client.codecs.span_context_from_string') as \
            from_str:
        from_str.return_value = (0, 1, 1, 1)  # make trace ID == 0 (i.e. invalid)
        span_context = codec.extract(headers)
        span = tracer.start_span('test', child_of=span_context)
        assert span.context.baggage == match
        # also check baggage through API
        for k, v in match.items():
            assert span.get_baggage_item(k) == v


@pytest.mark.parametrize('fmt,carrier', [
    (Format.TEXT_MAP, {}),
    (Format.HTTP_HEADERS, {}),
    (ZipkinSpanFormat, {}),
])
def test_round_trip(tracer, fmt, carrier):
    tracer_128bit = Tracer(
        service_name='test',
        reporter=InMemoryReporter(),
        sampler=ConstSampler(True),
        generate_128bit_trace_id=True)

    for tracer1, tracer2 in product([tracer, tracer_128bit], repeat=2):
        span = tracer1.start_span('test-%s' % fmt)
        tracer1.inject(span, fmt, carrier)
        context = tracer2.extract(fmt, carrier)
        span2 = tracer2.start_span('test-%s' % fmt, child_of=context)
        assert span.trace_id == span2.trace_id


def _text_codec_to_trace_id_string(carrier):
    return carrier[constants.TRACE_ID_HEADER].split(':')[0]


def _zipkin_codec_to_trace_id_string(carrier):
    return '{:x}'.format(carrier['trace_id'])


@pytest.mark.parametrize('fmt,carrier,get_trace_id', [
    (Format.TEXT_MAP, {}, _text_codec_to_trace_id_string),
    (Format.HTTP_HEADERS, {}, _text_codec_to_trace_id_string),
    (ZipkinSpanFormat, {}, _zipkin_codec_to_trace_id_string),
])
def test_inject_with_128bit_trace_id(tracer, fmt, carrier, get_trace_id):
    tracer_128bit = Tracer(
        service_name='test',
        reporter=InMemoryReporter(),
        sampler=ConstSampler(True),
        generate_128bit_trace_id=True)

    for tracer in [tracer, tracer_128bit]:
        length = tracer.max_trace_id_bits / 4
        trace_id = (1 << 64) - 1 if length == 16 else (1 << 128) - 1
        ctx = SpanContext(trace_id=trace_id, span_id=127, parent_id=None,
                          flags=1)
        span = Span(ctx, operation_name='test-%s' % fmt, tracer=None, start_time=1)
        tracer.inject(span, fmt, carrier)
        assert len(get_trace_id(carrier)) == length

        # test if the trace_id arrived on wire remains same even if
        # the tracer is configured for 64bit ids or 128bit ids
        ctx = SpanContext(trace_id=(1 << 128) - 1, span_id=127, parent_id=None,
                          flags=0)
        span = tracer.start_span('test-%s' % fmt, child_of=ctx)
        carrier = dict()
        tracer.inject(span, fmt, carrier)
        assert len(get_trace_id(carrier)) == 32

        ctx = SpanContext(trace_id=(1 << 64) - 1, span_id=127, parent_id=None,
                          flags=0)
        span = tracer.start_span('test-%s' % fmt, child_of=ctx)
        carrier = dict()
        tracer.inject(span, fmt, carrier)
        assert len(get_trace_id(carrier)) == 16


def test_debug_id():
    debug_header = 'correlation-id'
    tracer = Tracer(
        service_name='test',
        reporter=InMemoryReporter(),
        sampler=ConstSampler(True),
        debug_id_header=debug_header,
    )
    tracer.codecs[Format.TEXT_MAP] = TextCodec(
        url_encoding=False,
        debug_id_header=debug_header,
    )
    carrier = {debug_header: 'Coraline'}
    context = tracer.extract(Format.TEXT_MAP, carrier)
    assert context.is_debug_id_container_only
    assert context.debug_id == 'Coraline'
    span = tracer.start_span('test', child_of=context)
    assert span.is_debug()
    assert span.is_sampled()
    tags = [t for t in span.tags if t.key == debug_header]
    assert len(tags) == 1
    assert tags[0].vStr == 'Coraline'


def test_baggage_as_unicode_strings_with_httplib(httpserver):
    import urllib.request
    urllib_under_test = urllib.request

    # httpserver is provided by pytest-localserver
    httpserver.serve_content(content='Hello', code=200, headers=None)

    tracer = Tracer(
        service_name='test',
        reporter=InMemoryReporter(),
        # don't sample to avoid logging baggage to the span
        sampler=ConstSampler(False),
    )
    tracer.codecs[Format.TEXT_MAP] = TextCodec(url_encoding=True)

    baggage = [
        (b'key1', b'value'),
        (u'key2', b'value'),
        ('key3', u'value'),
        (b'key4', bytes([255])),
        (u'key5', u'\U0001F47E')
    ]
    for b in baggage:
        span = tracer.start_span('test')
        span.set_baggage_item(b[0], b[1])

        headers = {}
        tracer.inject(span_context=span.context,
                      format=Format.TEXT_MAP,
                      carrier=headers)
        # make sure httplib doesn't blow up
        request = urllib_under_test.Request(httpserver.url, None, headers)
        response = urllib_under_test.urlopen(request)
        assert response.read() == b'Hello'
        response.close()
