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
)

# pylint: disable=invalid-name
CallbackT = (
    Callable[[CallbackOptions], Iterable[Observation]]
    | Generator[Iterable[Observation], CallbackOptions, None]
)


CPU_FREQUENCY: Final = "cpu.frequency"
"""
Deprecated: Replaced by `system.cpu.frequency`.
"""


def create_cpu_frequency(
    meter: Meter, callbacks: Sequence[CallbackT] | None
) -> ObservableGauge:
    """Deprecated. Use `system.cpu.frequency` instead"""
    return meter.create_observable_gauge(
        name=CPU_FREQUENCY,
        callbacks=callbacks,
        description="Deprecated. Use `system.cpu.frequency` instead.",
        unit="{Hz}",
    )


CPU_TIME: Final = "cpu.time"
"""
Deprecated: Replaced by `system.cpu.time`.
"""


def create_cpu_time(meter: Meter) -> Counter:
    """Deprecated. Use `system.cpu.time` instead"""
    return meter.create_counter(
        name=CPU_TIME,
        description="Deprecated. Use `system.cpu.time` instead.",
        unit="s",
    )


CPU_UTILIZATION: Final = "cpu.utilization"
"""
Deprecated: Replaced by `system.cpu.utilization`.
"""


def create_cpu_utilization(
    meter: Meter, callbacks: Sequence[CallbackT] | None
) -> ObservableGauge:
    """Deprecated. Use `system.cpu.utilization` instead"""
    return meter.create_observable_gauge(
        name=CPU_UTILIZATION,
        callbacks=callbacks,
        description="Deprecated. Use `system.cpu.utilization` instead.",
        unit="1",
    )
