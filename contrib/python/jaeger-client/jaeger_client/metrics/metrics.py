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

from typing import Any, Optional, Callable, Dict


class MetricsFactory(object):
    """Generates new metrics."""

    def _noop(self, *args):
        pass

    def create_counter(
        self, name: str, tags: Optional[Dict[str, str]] = None
    ) -> Callable[[int], None]:
        """
        Generates a new counter from the given name and tags and returns
        a callable function used to increment the counter.
        :param name: name of the counter
        :param tags: tags for the counter
        :return: a callable function which takes the value to increase
        the counter by ie. def increment(value)
        """
        return self._noop

    def create_timer(
        self, name: str, tags: Optional[Dict[str, str]] = None
    ) -> Callable[[float], None]:
        """
        Generates a new timer from the given name and tags and returns
        a callable function used to record a float duration in microseconds.
        :param name: name of the timer
        :param tags: tags for the timer
        :return: a callable function which takes the duration to
        record ie. def record(duration)
        """
        return self._noop

    def create_gauge(
        self, name: str, tags: Optional[Dict[str, str]] = None
    ) -> Callable[[float], None]:
        """
        Generates a new gauge from the given name and tags and returns
        a callable function used to update the gauge.
        :param name: name of the gauge
        :param tags: tags for the gauge
        :return: a callable function which takes the value to update
        the gauge with ie. def update(value)
        """
        return self._noop


class LegacyMetricsFactory(MetricsFactory):
    """A MetricsFactory adapter for legacy Metrics class."""

    def __init__(self, metrics: 'Metrics') -> None:
        self._metrics = metrics

    def create_counter(
        self, name: str, tags: Optional[Dict[str, str]] = None
    ) -> Callable[[int], None]:
        key = self._get_key(name, tags)

        def increment(value: int) -> Optional[Any]:
            return self._metrics.count(key, value)
        return increment

    def create_timer(
        self, name: str, tags: Optional[Dict[str, str]] = None
    ) -> Callable[[float], None]:
        key = self._get_key(name, tags)

        def record(value):
            # Convert microseconds to milliseconds for legacy
            return self._metrics.timing(key, value / 1000.0)
        return record

    def create_gauge(
        self, name: str, tags: Optional[Dict[str, str]] = None
    ) -> Callable[[float], None]:
        key = self._get_key(name, tags)

        def update(value):
            return self._metrics.gauge(key, value)
        return update

    def _get_key(self, name, tags=None):
        if not tags:
            return name
        key = name
        for k in sorted(tags.keys()):
            key = key + '.' + str(k) + '_' + str(tags[k])
        return key


class Metrics(object):
    """
    Provides an abstraction of metrics reporting framework.
    This Class has been deprecated, please use MetricsFactory
    instead.
    """

    def __init__(self, count=None, gauge=None, timing=None):
        """
        :param count: function (key, value) to emit counters
        :param gauge: function (key, value) to emit gauges
        :param timing: function (key, value in milliseconds) to
            emit timings
        """
        self._count = count
        self._gauge = gauge
        self._timing = timing
        if not callable(self._count):
            self._count = None
        if not callable(self._gauge):
            self._gauge = None
        if not callable(self._timing):
            self._timing = None

    def count(self, key, value):
        if self._count:
            self._count(key, value)

    def timing(self, key, value):
        if self._timing:
            self._timing(key, value)

    def gauge(self, key, value):
        if self._gauge:
            self._gauge(key, value)
