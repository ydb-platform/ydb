# Copyright (c) 2018, The Jaeger Authors
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

from jaeger_client.metrics.prometheus \
    import PrometheusMetricsFactory
from prometheus_client import REGISTRY


def test_prometheus_metrics_counter():
    metrics = PrometheusMetricsFactory(namespace='test')
    counter1 = metrics.create_counter(name='jaeger:test_counter',
                                      tags={'result': 'ok'})
    counter1(1)
    counter2 = metrics.create_counter(name='jaeger:test_counter',
                                      tags={'result': 'ok'})
    counter2(1)
    after = REGISTRY.get_sample_value('test_jaeger:test_counter_total',
                                      {'result': 'ok'})
    assert 2 == after


def test_prometheus_metrics_counter_without_tags():
    metrics = PrometheusMetricsFactory()
    counter = metrics.create_counter(name='jaeger:test_counter_no_tags')
    counter(1)
    after = REGISTRY.get_sample_value('jaeger:test_counter_no_tags_total')
    assert 1 == after


def test_prometheus_metrics_guage():
    metrics = PrometheusMetricsFactory(namespace='test')
    gauge = metrics.create_gauge(name='jaeger:test_gauge',
                                 tags={'result': 'ok'})
    gauge(1)
    after = REGISTRY.get_sample_value('test_jaeger:test_gauge',
                                      {'result': 'ok'})
    assert 1 == after


def test_prometheus_metrics_gauge_without_tags():
    metrics = PrometheusMetricsFactory()
    gauge = metrics.create_gauge(name='jaeger:test_gauge_no_tags')
    gauge(1)
    after = REGISTRY.get_sample_value('jaeger:test_gauge_no_tags')
    assert 1 == after


def test_prometheus_metrics_metric_with_service_name_label():
    metrics = PrometheusMetricsFactory(service_name_label='test')
    gauge = metrics.create_gauge(name='jaeger:test_gauge_with_service_name_label')
    gauge(1)
    gauge_after = REGISTRY.get_sample_value('jaeger:test_gauge_with_service_name_label',
                                            {'service': 'test'})
    counter = metrics.create_counter(name='jaeger:test_counter_with_service_name_label',
                                     tags={'x': 'y'})
    counter(1)
    counter_after = REGISTRY.get_sample_value('jaeger:test_counter_with_service_name_label_total',
                                              {'service': 'test', 'x': 'y'})
    assert 1 == counter_after
    assert 1 == gauge_after


def test_prometheus_metrics_metric_without_service_name_label():
    metrics = PrometheusMetricsFactory()
    gauge = metrics.create_gauge(name='jaeger:test_gauge_without_service_name_label')
    gauge(1)
    gauge_after = REGISTRY.get_sample_value('jaeger:test_gauge_without_service_name_label')
    counter = metrics.create_counter(name='jaeger:test_counter_without_service_name_label')
    counter(1)
    counter_after = REGISTRY.get_sample_value(
        'jaeger:test_counter_without_service_name_label_total'
    )
    assert 1 == counter_after
    assert 1 == gauge_after
