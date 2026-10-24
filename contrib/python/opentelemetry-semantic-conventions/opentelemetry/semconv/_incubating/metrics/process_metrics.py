# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


from collections.abc import Callable, Generator, Iterable, Sequence
from typing import Final

from opentelemetry.metrics import (
    CallbackOptions,
    Counter,
    Meter,
    ObservableGauge,
    Observation,
    UpDownCounter,
)

# pylint: disable=invalid-name
CallbackT = (
    Callable[[CallbackOptions], Iterable[Observation]]
    | Generator[Iterable[Observation], CallbackOptions, None]
)


PROCESS_CONTEXT_SWITCHES: Final = "process.context_switches"
"""
Number of times the process has been context switched
Instrument: counter
Unit: {context_switch}
"""


def create_process_context_switches(meter: Meter) -> Counter:
    """Number of times the process has been context switched"""
    return meter.create_counter(
        name=PROCESS_CONTEXT_SWITCHES,
        description="Number of times the process has been context switched.",
        unit="{context_switch}",
    )


PROCESS_CPU_TIME: Final = "process.cpu.time"
"""
Total CPU seconds broken down by different CPU modes
Instrument: counter
Unit: s
"""


def create_process_cpu_time(meter: Meter) -> Counter:
    """Total CPU seconds broken down by different CPU modes"""
    return meter.create_counter(
        name=PROCESS_CPU_TIME,
        description="Total CPU seconds broken down by different CPU modes.",
        unit="s",
    )


PROCESS_CPU_UTILIZATION: Final = "process.cpu.utilization"
"""
Difference in process.cpu.time since the last measurement, divided by the elapsed time and number of CPUs available to the process
Instrument: gauge
Unit: 1
"""


def create_process_cpu_utilization(
    meter: Meter, callbacks: Sequence[CallbackT] | None
) -> ObservableGauge:
    """Difference in process.cpu.time since the last measurement, divided by the elapsed time and number of CPUs available to the process"""
    return meter.create_observable_gauge(
        name=PROCESS_CPU_UTILIZATION,
        callbacks=callbacks,
        description="Difference in process.cpu.time since the last measurement, divided by the elapsed time and number of CPUs available to the process.",
        unit="1",
    )


PROCESS_DISK_IO: Final = "process.disk.io"
"""
Disk bytes transferred
Instrument: counter
Unit: By
"""


def create_process_disk_io(meter: Meter) -> Counter:
    """Disk bytes transferred"""
    return meter.create_counter(
        name=PROCESS_DISK_IO,
        description="Disk bytes transferred.",
        unit="By",
    )


PROCESS_MEMORY_USAGE: Final = "process.memory.usage"
"""
The amount of physical memory in use
Instrument: updowncounter
Unit: By
"""


def create_process_memory_usage(meter: Meter) -> UpDownCounter:
    """The amount of physical memory in use"""
    return meter.create_up_down_counter(
        name=PROCESS_MEMORY_USAGE,
        description="The amount of physical memory in use.",
        unit="By",
    )


PROCESS_MEMORY_VIRTUAL: Final = "process.memory.virtual"
"""
The amount of committed virtual memory
Instrument: updowncounter
Unit: By
"""


def create_process_memory_virtual(meter: Meter) -> UpDownCounter:
    """The amount of committed virtual memory"""
    return meter.create_up_down_counter(
        name=PROCESS_MEMORY_VIRTUAL,
        description="The amount of committed virtual memory.",
        unit="By",
    )


PROCESS_NETWORK_IO: Final = "process.network.io"
"""
Network bytes transferred
Instrument: counter
Unit: By
"""


def create_process_network_io(meter: Meter) -> Counter:
    """Network bytes transferred"""
    return meter.create_counter(
        name=PROCESS_NETWORK_IO,
        description="Network bytes transferred.",
        unit="By",
    )


PROCESS_OPEN_FILE_DESCRIPTOR_COUNT: Final = (
    "process.open_file_descriptor.count"
)
"""
Deprecated: Replaced by `process.unix.file_descriptor.count`.
"""


def create_process_open_file_descriptor_count(meter: Meter) -> UpDownCounter:
    """Deprecated, use `process.unix.file_descriptor.count` instead"""
    return meter.create_up_down_counter(
        name=PROCESS_OPEN_FILE_DESCRIPTOR_COUNT,
        description="Deprecated, use `process.unix.file_descriptor.count` instead.",
        unit="{file_descriptor}",
    )


PROCESS_PAGING_FAULTS: Final = "process.paging.faults"
"""
Number of page faults the process has made
Instrument: counter
Unit: {fault}
"""


def create_process_paging_faults(meter: Meter) -> Counter:
    """Number of page faults the process has made"""
    return meter.create_counter(
        name=PROCESS_PAGING_FAULTS,
        description="Number of page faults the process has made.",
        unit="{fault}",
    )


PROCESS_THREAD_COUNT: Final = "process.thread.count"
"""
Process threads count
Instrument: updowncounter
Unit: {thread}
"""


def create_process_thread_count(meter: Meter) -> UpDownCounter:
    """Process threads count"""
    return meter.create_up_down_counter(
        name=PROCESS_THREAD_COUNT,
        description="Process threads count.",
        unit="{thread}",
    )


PROCESS_UNIX_FILE_DESCRIPTOR_COUNT: Final = (
    "process.unix.file_descriptor.count"
)
"""
Number of unix file descriptors in use by the process
Instrument: updowncounter
Unit: {file_descriptor}
"""


def create_process_unix_file_descriptor_count(meter: Meter) -> UpDownCounter:
    """Number of unix file descriptors in use by the process"""
    return meter.create_up_down_counter(
        name=PROCESS_UNIX_FILE_DESCRIPTOR_COUNT,
        description="Number of unix file descriptors in use by the process.",
        unit="{file_descriptor}",
    )


PROCESS_UPTIME: Final = "process.uptime"
"""
The time the process has been running
Instrument: gauge
Unit: s
Note: Instrumentations SHOULD use a gauge with type `double` and measure uptime in seconds as a floating point number with the highest precision available.
The actual accuracy would depend on the instrumentation and operating system.
"""


def create_process_uptime(
    meter: Meter, callbacks: Sequence[CallbackT] | None
) -> ObservableGauge:
    """The time the process has been running"""
    return meter.create_observable_gauge(
        name=PROCESS_UPTIME,
        callbacks=callbacks,
        description="The time the process has been running.",
        unit="s",
    )


PROCESS_WINDOWS_HANDLE_COUNT: Final = "process.windows.handle.count"
"""
Number of handles held by the process
Instrument: updowncounter
Unit: {handle}
"""


def create_process_windows_handle_count(meter: Meter) -> UpDownCounter:
    """Number of handles held by the process"""
    return meter.create_up_down_counter(
        name=PROCESS_WINDOWS_HANDLE_COUNT,
        description="Number of handles held by the process.",
        unit="{handle}",
    )
