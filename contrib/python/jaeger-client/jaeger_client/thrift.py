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

import traceback
from opentracing.tracer import ReferenceType
from .constants import MAX_TRACEBACK_LENGTH

import jaeger_client.thrift_gen.jaeger.ttypes as ttypes
import jaeger_client.thrift_gen.sampling.SamplingManager as sampling_manager

_max_signed_port = (1 << 15) - 1
_max_unsigned_port = (1 << 16)
_max_signed_id = (1 << 63) - 1
_max_unsigned_id = (1 << 64)


def _id_to_low(big_id):
    """
    :param big_id: id in integer
    :return: Returns the right most 64 bits of big_id
    """
    if big_id is not None:
        return big_id & (_max_unsigned_id - 1)


def _id_to_high(big_id):
    """
    :param big_id: id in integer
    :return: Returns the left most 64 bits of big_id
    """
    if big_id is not None:
        return (big_id >> 64) & (_max_unsigned_id - 1)


def id_to_int(big_id):
    if big_id is None:
        return None
    # thrift defines ID fields as i64, which is signed,
    # therefore we convert large IDs (> 2^63) to negative longs
    if big_id > _max_signed_id:
        big_id -= _max_unsigned_id
    return big_id


def _to_string(s):
    try:
        return str(s)
    except Exception as e:
        return str(e)


def make_tag(key, value, max_length, max_traceback_length):
    if type(value).__name__ == 'bool':  # isinstance doesnt work on booleans
        return _make_bool_tag(
            key=key,
            value=value
        )
    elif isinstance(value, int):
        return _make_long_tag(
            key=key,
            value=value
        )
    elif isinstance(value, float):
        return _make_double_tag(
            key=key,
            value=value
        )
    elif type(value).__name__ == 'traceback':
        return _make_traceback_tag(
            key=key,
            value=value,
            max_length=max_traceback_length
        )
    else:
        return _make_string_tag(
            key=key,
            value=value,
            max_length=max_length
        )


def _make_traceback_tag(key, value, max_length):
    key = _to_string(key)
    value = ''.join(traceback.format_tb(value))
    if len(value) > max_length:
        value = value[:max_length]
    return ttypes.Tag(
        key=key,
        vStr=value,
        vType=ttypes.TagType.STRING,
    )


def _make_string_tag(key, value, max_length):
    key = _to_string(key)
    value = _to_string(value)
    if len(value) > max_length:
        value = value[:max_length]
    return ttypes.Tag(
        key=key,
        vStr=value,
        vType=ttypes.TagType.STRING,
    )


def _make_long_tag(key, value):
    key = _to_string(key)
    return ttypes.Tag(
        key=key,
        vLong=value,
        vType=ttypes.TagType.LONG
    )


def _make_double_tag(key, value):
    key = _to_string(key)
    return ttypes.Tag(
        key=key,
        vDouble=value,
        vType=ttypes.TagType.DOUBLE
    )


def _make_bool_tag(key, value):
    key = _to_string(key)
    return ttypes.Tag(
        key=key,
        vBool=value,
        vType=ttypes.TagType.BOOL
    )


def timestamp_micros(ts):
    """
    Convert a float Unix timestamp from time.time() into a int value
    in microseconds, as required by Zipkin protocol.
    :param ts:
    :return:
    """
    return int(ts * 1000000)


def make_tags(tags, max_length, max_traceback_length):
    # TODO extend to support non-string tag values
    return [
        make_tag(key=k, value=v, max_length=max_length,
                 max_traceback_length=max_traceback_length)
        for k, v in (tags or {}).items()
    ]


def make_log(timestamp, fields, max_length, max_traceback_length):
    return ttypes.Log(
        timestamp=timestamp_micros(ts=timestamp),
        fields=make_tags(tags=fields, max_length=max_length,
                         max_traceback_length=max_traceback_length),
    )


def make_process(service_name, tags, max_length):
    return ttypes.Process(
        serviceName=service_name,
        tags=make_tags(tags=tags, max_length=max_length,
                       max_traceback_length=MAX_TRACEBACK_LENGTH),
    )


def make_ref_type(span_ref_type):
    if span_ref_type == ReferenceType.FOLLOWS_FROM:
        return ttypes.SpanRefType.FOLLOWS_FROM
    return ttypes.SpanRefType.CHILD_OF


def make_references(references):
    if not references:
        return None
    list_of_span_refs = list()
    for span_ref in references:
        list_of_span_refs.append(ttypes.SpanRef(
            refType=make_ref_type(span_ref.type),
            traceIdLow=id_to_int(_id_to_low(span_ref.referenced_context.trace_id)),
            traceIdHigh=id_to_int(_id_to_high(span_ref.referenced_context.trace_id)),
            spanId=id_to_int(span_ref.referenced_context.span_id),
        ))
    return list_of_span_refs


def make_jaeger_batch(spans, process):
    batch = ttypes.Batch(
        spans=[],
        process=process,
    )
    for span in spans:
        with span.update_lock:
            jaeger_span = ttypes.Span(
                traceIdLow=id_to_int(_id_to_low(span.trace_id)),
                traceIdHigh=id_to_int(_id_to_high(span.trace_id)),
                spanId=id_to_int(span.span_id),
                parentSpanId=id_to_int(span.parent_id) or 0,
                operationName=span.operation_name,
                references=make_references(span.references),
                flags=span.context.flags,
                startTime=timestamp_micros(span.start_time),
                duration=timestamp_micros(span.end_time - span.start_time),
                tags=span.tags,  # TODO
                logs=span.logs,  # TODO
            )
        batch.spans.append(jaeger_span)
    return batch


def parse_sampling_strategy(response):
    """
    Parse SamplingStrategyResponse and converts to a Sampler.

    :param response:
    :return: Returns Go-style (value, error) pair
    """
    s_type = response.strategyType
    if s_type == sampling_manager.SamplingStrategyType.PROBABILISTIC:
        if response.probabilisticSampling is None:
            return None, 'probabilisticSampling field is None'
        sampling_rate = response.probabilisticSampling.samplingRate
        if 0 <= sampling_rate <= 1.0:
            from jaeger_client.sampler import ProbabilisticSampler
            return ProbabilisticSampler(rate=sampling_rate), None
        return None, (
            'Probabilistic sampling rate not in [0, 1] range: %s' %
            sampling_rate
        )
    elif s_type == sampling_manager.SamplingStrategyType.RATE_LIMITING:
        if response.rateLimitingSampling is None:
            return None, 'rateLimitingSampling field is None'
        mtps = response.rateLimitingSampling.maxTracesPerSecond
        if 0 <= mtps < 500:
            from jaeger_client.sampler import RateLimitingSampler
            return RateLimitingSampler(max_traces_per_second=mtps), None
        return None, (
            'Rate limiting parameter not in [0, 500] range: %s' % mtps
        )
    return None, (
        'Unsupported sampling strategy type: %s' % s_type
    )
