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

from jaeger_client.metrics import MetricsFactory
from collections import defaultdict
from prometheus_client import Counter, Gauge
from typing import Any, Optional, Dict, Callable, DefaultDict


class PrometheusMetricsFactory(MetricsFactory):
    """
    Provides metrics backed by Prometheus
    """
    def __init__(self, namespace: str = '', service_name_label: Optional[str] = None) -> None:
        self._cache: DefaultDict = defaultdict(object)
        self._namespace = namespace
        self._service_name_label = service_name_label

    def _get_tag_name_list(self, tags):
        if tags is None:
            return []
        tag_name_list = []
        for key in tags.keys():
            tag_name_list.append(key)
        return tag_name_list

    def _get_metric(self, metricType, name, tags):
        if self._service_name_label:
            if tags is None:
                tags = {'service': self._service_name_label}
            else:
                tags['service'] = self._service_name_label

        label_name_list = self._get_tag_name_list(tags)
        cache_key = name + '@@'.join(label_name_list)

        metric = self._cache.get(cache_key)
        if metric is None:
            metric = metricType(name=name, documentation=name,
                                labelnames=label_name_list, namespace=self._namespace)
            self._cache[cache_key] = metric

        if tags is not None and len(tags) > 0:
            metric = metric.labels(**tags)

        return metric

    def create_counter(
        self, name: str, tags: Optional[Dict[str, Any]] = None
    ) -> Callable[[int], None]:
        counter = self._get_metric(Counter, name, tags)

        def increment(value):
            counter.inc(value)
        return increment

    def create_gauge(
        self, name: str, tags: Optional[Dict[str, Any]] = None
    ) -> Callable[[float], None]:
        gauge = self._get_metric(Gauge, name, tags)

        def update(value):
            gauge.set(value)
        return update
