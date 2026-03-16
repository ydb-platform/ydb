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

import time
from opentracing import Tracer as NoopTracer
from jaeger_client.tracer import Tracer
from jaeger_client.reporter import NullReporter
from jaeger_client.sampler import ConstSampler


def _generate_spans(tracer, iterations=1000, sleep=None):
    for i in range(0, iterations):
        span = tracer.start_trace(operation_name='root-span')
        span.finish()
        if sleep is not None:
            t = time.time() + sleep
            while time.time() < t:
                pass


def test_noop_tracer(benchmark):
    tracer = NoopTracer()
    benchmark(_generate_spans, tracer)


def test_no_sampling(benchmark):
    tracer = Tracer.default_tracer(
        channel=None, service_name='benchmark',
        reporter=NullReporter(), sampler=ConstSampler(False))
    benchmark(_generate_spans, tracer)


def test_100pct_sampling(benchmark):
    tracer = Tracer.default_tracer(
        channel=None, service_name='benchmark',
        reporter=NullReporter(), sampler=ConstSampler(True))
    benchmark(_generate_spans, tracer)


def test_100pct_sampling_250mcs(benchmark):
    tracer = Tracer.default_tracer(
        channel=None, service_name='benchmark',
        reporter=NullReporter(), sampler=ConstSampler(True))
    # 250 micros for request execution
    benchmark(_generate_spans, tracer, sleep=0.00025)


def test_all_batched_size10(benchmark):
    from tchannel.sync import TChannel
    ch = TChannel(name='foo')
    f = ch.advertise(routers=['127.0.0.1:21300', '127.0.0.1:21301'])
    f.result()
    tracer = Tracer.default_tracer(ch, service_name='benchmark',
                                   sampler=ConstSampler(True))
    tracer.reporter.batch_size = 10
    # 250 micros for request execution
    benchmark(_generate_spans, tracer, sleep=0.00025)


def test_all_batched_size5(benchmark):
    from tchannel.sync import TChannel
    ch = TChannel(name='foo')
    f = ch.advertise(routers=['127.0.0.1:21300', '127.0.0.1:21301'])
    f.result()
    tracer = Tracer.default_tracer(ch, service_name='benchmark',
                                   sampler=ConstSampler(True))
    tracer.reporter.batch_size = 5
    # 250 micros for request execution
    benchmark(_generate_spans, tracer, sleep=0.00025)


def test_all_not_batched(benchmark):
    from tchannel.sync import TChannel
    ch = TChannel(name='foo')
    f = ch.advertise(routers=['127.0.0.1:21300', '127.0.0.1:21301'])
    f.result()
    tracer = Tracer.default_tracer(ch, service_name='benchmark', sampler=ConstSampler(True))
    tracer.reporter.batch_size = 1
    # 250 micros for request execution
    benchmark(_generate_spans, tracer, sleep=0.00025)
