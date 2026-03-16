# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2016 Hynek Schlawack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from contextlib import suppress

import pytest


try:
    import twisted
except ImportError:
    twisted = None
    collect_ignore = ["tests/test_tx.py"]


def mk_monotonic_timer():
    """
    Create a function that always returns the next integer beginning at 0.
    """

    def timer():
        timer.i += 1
        return timer.i

    timer.i = 0

    return timer


class FakeObserver:
    """
    A fake metric observer that saves all observed values in a list.
    """

    def __init__(self):
        self._observed = []

    def observe(self, value):
        self._observed.append(value)


class FakeCounter:
    """
    A fake counter metric.
    """

    def __init__(self):
        self._val = 0

    def inc(self):
        self._val += 1


class FakeGauge:
    """
    A fake Gauge.
    """

    def __init__(self):
        self._val = 0
        self._calls = 0

    def inc(self, val=1):
        self._val += val
        self._calls += 1

    def dec(self, val=1):
        self._val -= val
        self._calls += 1


@pytest.fixture(autouse=True)
def _reset_registry(monkeypatch):
    """
    Ensures prometheus_client's CollectorRegistry is empty before each test.
    """
    from prometheus_client import REGISTRY

    for c in list(REGISTRY._collector_to_names):
        REGISTRY.unregister(c)


@pytest.fixture(name="fake_observer")
def _fake_observer():
    return FakeObserver()


@pytest.fixture(name="fake_counter")
def _fake_counter():
    return FakeCounter()


@pytest.fixture(name="fake_gauge")
def _fake_gauge():
    return FakeGauge()


@pytest.fixture(name="patch_timer")
def _patch_timer(monkeypatch):
    with suppress(ImportError):
        from prometheus_async.tx import _decorators

        monkeypatch.setattr(_decorators, "perf_counter", mk_monotonic_timer())

    with suppress(ImportError):
        from prometheus_async.aio import _decorators

        monkeypatch.setattr(_decorators, "perf_counter", mk_monotonic_timer())
