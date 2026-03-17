# Copyright The OpenTelemetry Authors
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
# pylint: disable=too-many-lines

"""
Instrument to report system (CPU, memory, network) and
process (CPU, memory, garbage collection) metrics. By default, the
following metrics are configured:

.. code:: python

    {
        "system.cpu.time": ["idle", "user", "system", "irq"],
        "system.cpu.utilization": ["idle", "user", "system", "irq"],
        "system.memory.usage": ["used", "free", "cached"],
        "system.memory.utilization": ["used", "free", "cached"],
        "system.swap.usage": ["used", "free"],
        "system.swap.utilization": ["used", "free"],
        "system.disk.io": ["read", "write"],
        "system.disk.operations": ["read", "write"],
        "system.disk.time": ["read", "write"],
        "system.network.dropped.packets": ["transmit", "receive"],
        "system.network.packets": ["transmit", "receive"],
        "system.network.errors": ["transmit", "receive"],
        "system.network.io": ["transmit", "receive"],
        "system.network.connections": ["family", "type"],
        "system.thread_count": None
        "process.context_switches": ["involuntary", "voluntary"],
        "process.cpu.time": ["user", "system"],
        "process.cpu.utilization": None,
        "process.memory.usage": None,
        "process.memory.virtual": None,
        "process.open_file_descriptor.count": None,
        "process.thread.count": None,
        "process.runtime.memory": ["rss", "vms"],
        "process.runtime.cpu.time": ["user", "system"],
        "process.runtime.gc_count": None,
        "cpython.gc.collections": None,
        "cpython.gc.collected_objects": None,
        "cpython.gc.uncollectable_objects": None,
        "process.runtime.thread_count": None,
        "process.runtime.cpu.utilization": None,
        "process.runtime.context_switches": ["involuntary", "voluntary"],
    }

Usage
-----

.. code:: python

    from opentelemetry.metrics import set_meter_provider
    from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader

    exporter = ConsoleMetricExporter()

    set_meter_provider(MeterProvider([PeriodicExportingMetricReader(exporter)]))
    SystemMetricsInstrumentor().instrument()

    # metrics are collected asynchronously
    input("...")

    # to configure custom metrics
    configuration = {
        "system.memory.usage": ["used", "free", "cached"],
        "system.cpu.time": ["idle", "user", "system", "irq"],
        "system.network.io": ["transmit", "receive"],
        "process.memory.usage": None,
        "process.memory.virtual": None,
        "process.cpu.time": ["user", "system"],
        "process.context_switches": ["involuntary", "voluntary"],
    }
    SystemMetricsInstrumentor(config=configuration).instrument()


Out-of-spec `process.runtime` prefixed metrics are deprecated and will be removed in future versions, users are encouraged to move
to the `process` metrics.

API
---
"""

from __future__ import annotations

import gc
import logging
import os
import sys
import threading
from platform import python_implementation
from typing import Any, Collection, Iterable

import psutil

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.system_metrics.package import _instruments
from opentelemetry.instrumentation.system_metrics.version import __version__
from opentelemetry.metrics import CallbackOptions, Observation, get_meter
from opentelemetry.semconv._incubating.metrics.process_metrics import (
    create_process_cpu_utilization,
)

_logger = logging.getLogger(__name__)


_DEFAULT_CONFIG: dict[str, list[str] | None] = {
    "system.cpu.time": ["idle", "user", "system", "irq"],
    "system.cpu.utilization": ["idle", "user", "system", "irq"],
    "system.memory.usage": ["used", "free", "cached"],
    "system.memory.utilization": ["used", "free", "cached"],
    "system.swap.usage": ["used", "free"],
    "system.swap.utilization": ["used", "free"],
    "system.disk.io": ["read", "write"],
    "system.disk.operations": ["read", "write"],
    "system.disk.time": ["read", "write"],
    "system.network.dropped.packets": ["transmit", "receive"],
    "system.network.packets": ["transmit", "receive"],
    "system.network.errors": ["transmit", "receive"],
    "system.network.io": ["transmit", "receive"],
    "system.network.connections": ["family", "type"],
    "system.thread_count": None,
    "process.context_switches": ["involuntary", "voluntary"],
    "process.cpu.time": ["user", "system"],
    "process.cpu.utilization": ["user", "system"],
    "process.memory.usage": None,
    "process.memory.virtual": None,
    "process.open_file_descriptor.count": None,
    "process.thread.count": None,
    "process.runtime.memory": ["rss", "vms"],
    "process.runtime.cpu.time": ["user", "system"],
    "process.runtime.gc_count": None,
    "cpython.gc.collections": None,
    "cpython.gc.collected_objects": None,
    "cpython.gc.uncollectable_objects": None,
    "process.runtime.thread_count": None,
    "process.runtime.cpu.utilization": None,
    "process.runtime.context_switches": ["involuntary", "voluntary"],
}

if sys.platform == "darwin":
    # see https://github.com/giampaolo/psutil/issues/1219
    _DEFAULT_CONFIG.pop("system.network.connections")


class SystemMetricsInstrumentor(BaseInstrumentor):
    def __init__(
        self,
        labels: dict[str, str] | None = None,
        config: dict[str, list[str] | None] | None = None,
    ):
        super().__init__()
        if config is None:
            self._config = _DEFAULT_CONFIG
        else:
            self._config = config
        self._labels = {} if labels is None else labels
        self._meter = None
        self._python_implementation = python_implementation().lower()

        self._proc = psutil.Process(os.getpid())

        self._system_cpu_time_labels = self._labels.copy()
        self._system_cpu_utilization_labels = self._labels.copy()

        self._system_memory_usage_labels = self._labels.copy()
        self._system_memory_utilization_labels = self._labels.copy()

        self._system_swap_usage_labels = self._labels.copy()
        self._system_swap_utilization_labels = self._labels.copy()

        self._system_disk_io_labels = self._labels.copy()
        self._system_disk_operations_labels = self._labels.copy()
        self._system_disk_time_labels = self._labels.copy()
        self._system_disk_merged_labels = self._labels.copy()

        self._system_network_dropped_packets_labels = self._labels.copy()
        self._system_network_packets_labels = self._labels.copy()
        self._system_network_errors_labels = self._labels.copy()
        self._system_network_io_labels = self._labels.copy()
        self._system_network_connections_labels = self._labels.copy()

        self._system_thread_count_labels = self._labels.copy()

        self._context_switches_labels = self._labels.copy()
        self._cpu_time_labels = self._labels.copy()
        self._cpu_utilization_labels = self._labels.copy()
        self._memory_usage_labels = self._labels.copy()
        self._memory_virtual_labels = self._labels.copy()
        self._open_file_descriptor_count_labels = self._labels.copy()
        self._thread_count_labels = self._labels.copy()

        self._runtime_memory_labels = self._labels.copy()
        self._runtime_cpu_time_labels = self._labels.copy()
        self._runtime_gc_count_labels = self._labels.copy()
        self._runtime_gc_collections_labels = self._labels.copy()
        self._runtime_gc_collected_objects_labels = self._labels.copy()
        self._runtime_gc_uncollectable_objects_labels = self._labels.copy()
        self._runtime_thread_count_labels = self._labels.copy()
        self._runtime_cpu_utilization_labels = self._labels.copy()
        self._runtime_context_switches_labels = self._labels.copy()

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any):
        # pylint: disable=too-many-branches,too-many-statements
        meter_provider = kwargs.get("meter_provider")
        self._meter = get_meter(
            __name__,
            __version__,
            meter_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        # system metrics

        if "system.cpu.time" in self._config:
            self._meter.create_observable_counter(
                name="system.cpu.time",
                callbacks=[self._get_system_cpu_time],
                description="System CPU time",
                unit="s",
            )

        # FIXME: double check this is divided by cpu core
        if "system.cpu.utilization" in self._config:
            self._meter.create_observable_gauge(
                name="system.cpu.utilization",
                callbacks=[self._get_system_cpu_utilization],
                description="System CPU utilization",
                unit="1",
            )

        if "system.memory.usage" in self._config:
            self._meter.create_observable_gauge(
                name="system.memory.usage",
                callbacks=[self._get_system_memory_usage],
                description="System memory usage",
                unit="By",
            )

        if "system.memory.utilization" in self._config:
            self._meter.create_observable_gauge(
                name="system.memory.utilization",
                callbacks=[self._get_system_memory_utilization],
                description="System memory utilization",
                unit="1",
            )

        # FIXME: system.swap is gone in favour of system.paging
        if "system.swap.usage" in self._config:
            self._meter.create_observable_gauge(
                name="system.swap.usage",
                callbacks=[self._get_system_swap_usage],
                description="System swap usage",
                unit="pages",
            )

        if "system.swap.utilization" in self._config:
            self._meter.create_observable_gauge(
                name="system.swap.utilization",
                callbacks=[self._get_system_swap_utilization],
                description="System swap utilization",
                unit="1",
            )

        # TODO Add _get_system_swap_page_faults

        # self._meter.create_observable_counter(
        #     name="system.swap.page_faults",
        #     callbacks=[self._get_system_swap_page_faults],
        #     description="System swap page faults",
        #     unit="faults",
        #     value_type=int,
        # )

        # TODO Add _get_system_swap_page_operations
        # self._meter.create_observable_counter(
        #     name="system.swap.page_operations",
        #     callbacks=self._get_system_swap_page_operations,
        #     description="System swap page operations",
        #     unit="operations",
        #     value_type=int,
        # )

        if "system.disk.io" in self._config:
            self._meter.create_observable_counter(
                name="system.disk.io",
                callbacks=[self._get_system_disk_io],
                description="System disk IO",
                unit="By",
            )

        if "system.disk.operations" in self._config:
            self._meter.create_observable_counter(
                name="system.disk.operations",
                callbacks=[self._get_system_disk_operations],
                description="System disk operations",
                unit="operations",
            )

        # FIXME: this has been replaced by system.disk.operation.time
        if "system.disk.time" in self._config:
            self._meter.create_observable_counter(
                name="system.disk.time",
                callbacks=[self._get_system_disk_time],
                description="System disk time",
                unit="s",
            )

        # TODO Add _get_system_filesystem_usage

        # self.accumulator.register_valueobserver(
        #     callback=self._get_system_filesystem_usage,
        #     name="system.filesystem.usage",
        #     description="System filesystem usage",
        #     unit="By",
        #     value_type=int,
        # )

        # TODO Add _get_system_filesystem_utilization
        # self._meter.create_observable_gauge(
        #     callback=self._get_system_filesystem_utilization,
        #     name="system.filesystem.utilization",
        #     description="System filesystem utilization",
        #     unit="1",
        #     value_type=float,
        # )

        # TODO Filesystem information can be obtained with os.statvfs in Unix-like
        # OSs, how to do the same in Windows?

        # FIXME: this is now just system.network.dropped
        if "system.network.dropped.packets" in self._config:
            self._meter.create_observable_counter(
                name="system.network.dropped_packets",
                callbacks=[self._get_system_network_dropped_packets],
                description="System network dropped_packets",
                unit="packets",
            )

        if "system.network.packets" in self._config:
            self._meter.create_observable_counter(
                name="system.network.packets",
                callbacks=[self._get_system_network_packets],
                description="System network packets",
                unit="packets",
            )

        if "system.network.errors" in self._config:
            self._meter.create_observable_counter(
                name="system.network.errors",
                callbacks=[self._get_system_network_errors],
                description="System network errors",
                unit="errors",
            )

        if "system.network.io" in self._config:
            self._meter.create_observable_counter(
                name="system.network.io",
                callbacks=[self._get_system_network_io],
                description="System network io",
                unit="By",
            )

        if "system.network.connections" in self._config:
            self._meter.create_observable_up_down_counter(
                name="system.network.connections",
                callbacks=[self._get_system_network_connections],
                description="System network connections",
                unit="connections",
            )

        # FIXME: this is gone
        if "system.thread_count" in self._config:
            self._meter.create_observable_gauge(
                name="system.thread_count",
                callbacks=[self._get_system_thread_count],
                description="System active threads count",
            )

        # process metrics

        if "process.cpu.time" in self._config:
            self._meter.create_observable_counter(
                name="process.cpu.time",
                callbacks=[self._get_cpu_time],
                description="Total CPU seconds broken down by different states.",
                unit="s",
            )

        if "process.cpu.utilization" in self._config:
            create_process_cpu_utilization(
                self._meter, callbacks=[self._get_cpu_utilization]
            )

        if (
            "process.context_switches" in self._config
            and self._can_read_context_switches()
        ):
            self._meter.create_observable_counter(
                name="process.context_switches",
                callbacks=[self._get_context_switches],
                description="Number of times the process has been context switched.",
            )

        if "process.memory.usage" in self._config:
            self._meter.create_observable_up_down_counter(
                name="process.memory.usage",
                callbacks=[self._get_memory_usage],
                description="The amount of physical memory in use.",
                unit="By",
            )

        if "process.memory.virtual" in self._config:
            self._meter.create_observable_up_down_counter(
                name="process.memory.virtual",
                callbacks=[self._get_memory_virtual],
                description="The amount of committed virtual memory.",
                unit="By",
            )

        if (
            sys.platform != "win32"
            and "process.open_file_descriptor.count" in self._config
        ):
            self._meter.create_observable_up_down_counter(
                name="process.open_file_descriptor.count",
                callbacks=[self._get_open_file_descriptors],
                description="Number of file descriptors in use by the process.",
            )

        if "process.thread.count" in self._config:
            self._meter.create_observable_up_down_counter(
                name="process.thread.count",
                callbacks=[self._get_thread_count],
                description="Process threads count.",
            )

        # FIXME: process.runtime keys are deprecated and will be removed in subsequent releases.
        # When removing them, remember to clean also the callbacks and labels

        if "process.runtime.memory" in self._config:
            self._meter.create_observable_up_down_counter(
                name=f"process.runtime.{self._python_implementation}.memory",
                callbacks=[self._get_runtime_memory],
                description=f"Runtime {self._python_implementation} memory",
                unit="By",
            )

        if "process.runtime.cpu.time" in self._config:
            self._meter.create_observable_counter(
                name=f"process.runtime.{self._python_implementation}.cpu_time",
                callbacks=[self._get_runtime_cpu_time],
                description=f"Runtime {self._python_implementation} CPU time",
                unit="s",
            )

        if "process.runtime.gc_count" in self._config:
            if self._python_implementation == "pypy":
                _logger.warning(
                    "The process.runtime.gc_count metric won't be collected because the interpreter is PyPy"
                )
            else:
                self._meter.create_observable_counter(
                    name=f"process.runtime.{self._python_implementation}.gc_count",
                    callbacks=[self._get_runtime_gc_count],
                    description=f"Runtime {self._python_implementation} GC count",
                    unit="By",
                )

        if "cpython.gc.collections" in self._config:
            if self._python_implementation == "pypy":
                _logger.warning(
                    "The cpython.gc.collections metric won't be collected because the interpreter is PyPy"
                )
            else:
                self._meter.create_observable_counter(
                    name="cpython.gc.collections",
                    callbacks=[self._get_runtime_gc_collections],
                    description="The number of times a generation was collected since interpreter start.",
                    unit="{collection}",
                )

        if "cpython.gc.collected_objects" in self._config:
            if self._python_implementation == "pypy":
                _logger.warning(
                    "The cpython.gc.collected_objects metric won't be collected because the interpreter is PyPy"
                )
            else:
                self._meter.create_observable_counter(
                    name="cpython.gc.collected_objects",
                    callbacks=[self._get_runtime_gc_collected_objects],
                    description="The total number of objects collected since interpreter start.",
                    unit="{object}",
                )

        if "cpython.gc.uncollectable_objects" in self._config:
            if self._python_implementation == "pypy":
                _logger.warning(
                    "The cpython.gc.uncollectable_objects metric won't be collected because the interpreter is PyPy"
                )
            else:
                self._meter.create_observable_counter(
                    name="cpython.gc.uncollectable_objects",
                    callbacks=[self._get_runtime_gc_uncollectable_objects],
                    description="The total number of uncollectable objects found since interpreter start.",
                    unit="{object}",
                )

        if "process.runtime.thread_count" in self._config:
            self._meter.create_observable_up_down_counter(
                name=f"process.runtime.{self._python_implementation}.thread_count",
                callbacks=[self._get_runtime_thread_count],
                description="Runtime active threads count",
            )

        if "process.runtime.cpu.utilization" in self._config:
            self._meter.create_observable_gauge(
                name=f"process.runtime.{self._python_implementation}.cpu.utilization",
                callbacks=[self._get_runtime_cpu_utilization],
                description="Runtime CPU utilization",
                unit="1",
            )

        if (
            "process.runtime.context_switches" in self._config
            and self._can_read_context_switches()
        ):
            self._meter.create_observable_counter(
                name=f"process.runtime.{self._python_implementation}.context_switches",
                callbacks=[self._get_runtime_context_switches],
                description="Runtime context switches",
                unit="switches",
            )

    def _uninstrument(self, **kwargs: Any):
        pass

    def _can_read_context_switches(self) -> bool:
        """On Google Cloud Run psutil is not able to read context switches, catch it before creating the observable instrument"""
        try:
            self._proc.num_ctx_switches()
            return True
        except NotImplementedError:
            return False

    def _get_open_file_descriptors(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for Number of file descriptors in use by the process"""
        yield Observation(
            self._proc.num_fds(),
            self._open_file_descriptor_count_labels.copy(),
        )

    def _get_system_cpu_time(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for system CPU time"""
        for cpu, times in enumerate(psutil.cpu_times(percpu=True)):
            for metric in self._config["system.cpu.time"]:
                if hasattr(times, metric):
                    self._system_cpu_time_labels["state"] = metric
                    self._system_cpu_time_labels["cpu"] = cpu + 1
                    yield Observation(
                        getattr(times, metric),
                        self._system_cpu_time_labels.copy(),
                    )

    def _get_system_cpu_utilization(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for system CPU utilization"""

        for cpu, times_percent in enumerate(
            psutil.cpu_times_percent(percpu=True)
        ):
            for metric in self._config["system.cpu.utilization"]:
                if hasattr(times_percent, metric):
                    self._system_cpu_utilization_labels["state"] = metric
                    self._system_cpu_utilization_labels["cpu"] = cpu + 1
                    yield Observation(
                        getattr(times_percent, metric) / 100,
                        self._system_cpu_utilization_labels.copy(),
                    )

    def _get_system_memory_usage(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for memory usage"""
        virtual_memory = psutil.virtual_memory()
        for metric in self._config["system.memory.usage"]:
            self._system_memory_usage_labels["state"] = metric
            if hasattr(virtual_memory, metric):
                yield Observation(
                    getattr(virtual_memory, metric),
                    self._system_memory_usage_labels.copy(),
                )

    def _get_system_memory_utilization(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for memory utilization"""
        system_memory = psutil.virtual_memory()

        for metric in self._config["system.memory.utilization"]:
            self._system_memory_utilization_labels["state"] = metric
            if hasattr(system_memory, metric):
                yield Observation(
                    getattr(system_memory, metric) / system_memory.total,
                    self._system_memory_utilization_labels.copy(),
                )

    def _get_system_swap_usage(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for swap usage"""
        system_swap = psutil.swap_memory()

        for metric in self._config["system.swap.usage"]:
            self._system_swap_usage_labels["state"] = metric
            if hasattr(system_swap, metric):
                yield Observation(
                    getattr(system_swap, metric),
                    self._system_swap_usage_labels.copy(),
                )

    def _get_system_swap_utilization(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for swap utilization"""
        system_swap = psutil.swap_memory()

        for metric in self._config["system.swap.utilization"]:
            if hasattr(system_swap, metric):
                self._system_swap_utilization_labels["state"] = metric
                yield Observation(
                    (
                        getattr(system_swap, metric) / system_swap.total
                        if system_swap.total
                        else 0
                    ),
                    self._system_swap_utilization_labels.copy(),
                )

    def _get_system_disk_io(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for disk IO"""
        for device, counters in psutil.disk_io_counters(perdisk=True).items():
            for metric in self._config["system.disk.io"]:
                if hasattr(counters, f"{metric}_bytes"):
                    self._system_disk_io_labels["device"] = device
                    self._system_disk_io_labels["direction"] = metric
                    yield Observation(
                        getattr(counters, f"{metric}_bytes"),
                        self._system_disk_io_labels.copy(),
                    )

    def _get_system_disk_operations(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for disk operations"""
        for device, counters in psutil.disk_io_counters(perdisk=True).items():
            for metric in self._config["system.disk.operations"]:
                if hasattr(counters, f"{metric}_count"):
                    self._system_disk_operations_labels["device"] = device
                    self._system_disk_operations_labels["direction"] = metric
                    yield Observation(
                        getattr(counters, f"{metric}_count"),
                        self._system_disk_operations_labels.copy(),
                    )

    def _get_system_disk_time(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for disk time"""
        for device, counters in psutil.disk_io_counters(perdisk=True).items():
            for metric in self._config["system.disk.time"]:
                if hasattr(counters, f"{metric}_time"):
                    self._system_disk_time_labels["device"] = device
                    self._system_disk_time_labels["direction"] = metric
                    yield Observation(
                        getattr(counters, f"{metric}_time") / 1000,
                        self._system_disk_time_labels.copy(),
                    )

    def _get_system_disk_merged(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for disk merged operations"""

        # FIXME The units in the spec is 1, it seems like it should be
        # operations or the value type should be Double

        for device, counters in psutil.disk_io_counters(perdisk=True).items():
            for metric in self._config["system.disk.time"]:
                if hasattr(counters, f"{metric}_merged_count"):
                    self._system_disk_merged_labels["device"] = device
                    self._system_disk_merged_labels["direction"] = metric
                    yield Observation(
                        getattr(counters, f"{metric}_merged_count"),
                        self._system_disk_merged_labels.copy(),
                    )

    def _get_system_network_dropped_packets(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for network dropped packets"""

        for device, counters in psutil.net_io_counters(pernic=True).items():
            for metric in self._config["system.network.dropped.packets"]:
                in_out = {"receive": "in", "transmit": "out"}[metric]
                if hasattr(counters, f"drop{in_out}"):
                    self._system_network_dropped_packets_labels["device"] = (
                        device
                    )
                    self._system_network_dropped_packets_labels[
                        "direction"
                    ] = metric
                    yield Observation(
                        getattr(counters, f"drop{in_out}"),
                        self._system_network_dropped_packets_labels.copy(),
                    )

    def _get_system_network_packets(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for network packets"""

        for device, counters in psutil.net_io_counters(pernic=True).items():
            for metric in self._config["system.network.packets"]:
                recv_sent = {"receive": "recv", "transmit": "sent"}[metric]
                if hasattr(counters, f"packets_{recv_sent}"):
                    self._system_network_packets_labels["device"] = device
                    self._system_network_packets_labels["direction"] = metric
                    yield Observation(
                        getattr(counters, f"packets_{recv_sent}"),
                        self._system_network_packets_labels.copy(),
                    )

    def _get_system_network_errors(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for network errors"""
        for device, counters in psutil.net_io_counters(pernic=True).items():
            for metric in self._config["system.network.errors"]:
                in_out = {"receive": "in", "transmit": "out"}[metric]
                if hasattr(counters, f"err{in_out}"):
                    self._system_network_errors_labels["device"] = device
                    self._system_network_errors_labels["direction"] = metric
                    yield Observation(
                        getattr(counters, f"err{in_out}"),
                        self._system_network_errors_labels.copy(),
                    )

    def _get_system_network_io(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for network IO"""

        for device, counters in psutil.net_io_counters(pernic=True).items():
            for metric in self._config["system.network.io"]:
                recv_sent = {"receive": "recv", "transmit": "sent"}[metric]
                if hasattr(counters, f"bytes_{recv_sent}"):
                    self._system_network_io_labels["device"] = device
                    self._system_network_io_labels["direction"] = metric
                    yield Observation(
                        getattr(counters, f"bytes_{recv_sent}"),
                        self._system_network_io_labels.copy(),
                    )

    def _get_system_network_connections(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for network connections"""
        # TODO How to find the device identifier for a particular
        # connection?

        connection_counters = {}

        for net_connection in psutil.net_connections():
            for metric in self._config["system.network.connections"]:
                self._system_network_connections_labels["protocol"] = {
                    1: "tcp",
                    2: "udp",
                }[net_connection.type.value]
                self._system_network_connections_labels["state"] = (
                    net_connection.status
                )
                self._system_network_connections_labels[metric] = getattr(
                    net_connection, metric
                )

            connection_counters_key = tuple(
                sorted(self._system_network_connections_labels.items())
            )

            if connection_counters_key in connection_counters:
                connection_counters[connection_counters_key]["counter"] += 1
            else:
                connection_counters[connection_counters_key] = {
                    "counter": 1,
                    "labels": self._system_network_connections_labels.copy(),
                }

        for connection_counter in connection_counters.values():
            yield Observation(
                connection_counter["counter"],
                connection_counter["labels"],
            )

    def _get_system_thread_count(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for active thread count"""
        yield Observation(
            threading.active_count(), self._system_thread_count_labels
        )

    # process callbacks

    def _get_context_switches(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for context switches"""
        ctx_switches = self._proc.num_ctx_switches()
        for metric in self._config["process.context_switches"]:
            if hasattr(ctx_switches, metric):
                self._context_switches_labels["type"] = metric
                yield Observation(
                    getattr(ctx_switches, metric),
                    self._context_switches_labels.copy(),
                )

    def _get_cpu_time(self, options: CallbackOptions) -> Iterable[Observation]:
        """Observer callback for CPU time"""
        proc_cpu = self._proc.cpu_times()
        for metric in self._config["process.cpu.time"]:
            if hasattr(proc_cpu, metric):
                self._cpu_time_labels["type"] = metric
                yield Observation(
                    getattr(proc_cpu, metric),
                    self._cpu_time_labels.copy(),
                )

    def _get_cpu_utilization(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for CPU utilization"""
        proc_cpu_percent = self._proc.cpu_percent()
        # may return None so add a default of 1 in case
        num_cpus = psutil.cpu_count() or 1
        yield Observation(
            proc_cpu_percent / 100 / num_cpus,
            self._cpu_utilization_labels.copy(),
        )

    def _get_memory_usage(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for memory usage"""
        proc_memory = self._proc.memory_info()
        if hasattr(proc_memory, "rss"):
            yield Observation(
                getattr(proc_memory, "rss"),
                self._memory_usage_labels.copy(),
            )

    def _get_memory_virtual(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for memory virtual"""
        proc_memory = self._proc.memory_info()
        if hasattr(proc_memory, "vms"):
            yield Observation(
                getattr(proc_memory, "vms"),
                self._memory_virtual_labels.copy(),
            )

    def _get_thread_count(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for active thread count"""
        yield Observation(
            self._proc.num_threads(), self._thread_count_labels.copy()
        )

    # runtime callbacks

    def _get_runtime_memory(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for runtime memory"""
        proc_memory = self._proc.memory_info()
        for metric in self._config["process.runtime.memory"]:
            if hasattr(proc_memory, metric):
                self._runtime_memory_labels["type"] = metric
                yield Observation(
                    getattr(proc_memory, metric),
                    self._runtime_memory_labels.copy(),
                )

    def _get_runtime_cpu_time(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for runtime CPU time"""
        proc_cpu = self._proc.cpu_times()
        for metric in self._config["process.runtime.cpu.time"]:
            if hasattr(proc_cpu, metric):
                self._runtime_cpu_time_labels["type"] = metric
                yield Observation(
                    getattr(proc_cpu, metric),
                    self._runtime_cpu_time_labels.copy(),
                )

    def _get_runtime_gc_count(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for garbage collection"""
        for index, count in enumerate(gc.get_count()):
            self._runtime_gc_count_labels["count"] = str(index)
            yield Observation(count, self._runtime_gc_count_labels.copy())

    def _get_runtime_gc_collections(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for garbage collection"""
        for index, stat in enumerate(gc.get_stats()):
            self._runtime_gc_collections_labels["generation"] = str(index)
            yield Observation(
                stat["collections"], self._runtime_gc_collections_labels.copy()
            )

    def _get_runtime_gc_collected_objects(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for garbage collection collected objects"""
        for index, stat in enumerate(gc.get_stats()):
            self._runtime_gc_collected_objects_labels["generation"] = str(
                index
            )
            yield Observation(
                stat["collected"],
                self._runtime_gc_collected_objects_labels.copy(),
            )

    def _get_runtime_gc_uncollectable_objects(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for garbage collection uncollectable objects"""
        for index, stat in enumerate(gc.get_stats()):
            self._runtime_gc_uncollectable_objects_labels["generation"] = str(
                index
            )
            yield Observation(
                stat["uncollectable"],
                self._runtime_gc_uncollectable_objects_labels.copy(),
            )

    def _get_runtime_thread_count(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for runtime active thread count"""
        yield Observation(
            self._proc.num_threads(), self._runtime_thread_count_labels.copy()
        )

    def _get_runtime_cpu_utilization(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for runtime CPU utilization"""
        proc_cpu_percent = self._proc.cpu_percent()
        yield Observation(
            proc_cpu_percent / 100,
            self._runtime_cpu_utilization_labels.copy(),
        )

    def _get_runtime_context_switches(
        self, options: CallbackOptions
    ) -> Iterable[Observation]:
        """Observer callback for runtime context switches"""
        ctx_switches = self._proc.num_ctx_switches()
        for metric in self._config["process.runtime.context_switches"]:
            if hasattr(ctx_switches, metric):
                self._runtime_context_switches_labels["type"] = metric
                yield Observation(
                    getattr(ctx_switches, metric),
                    self._runtime_context_switches_labels.copy(),
                )
