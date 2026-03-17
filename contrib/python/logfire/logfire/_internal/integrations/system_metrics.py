from __future__ import annotations

import sys
from collections.abc import Iterable
from platform import python_implementation
from typing import TYPE_CHECKING, Literal, Optional, cast

from opentelemetry.metrics import CallbackOptions, Observation

if TYPE_CHECKING:
    from typing_extensions import LiteralString

    from logfire import Logfire

try:
    import psutil
    from opentelemetry.instrumentation.system_metrics import (
        _DEFAULT_CONFIG,  # type: ignore
        SystemMetricsInstrumentor,
    )
except ImportError as e:  # pragma: no cover
    raise RuntimeError(
        '`logfire.instrument_system_metrics()` requires the `opentelemetry-instrumentation-system-metrics` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[system-metrics]'"
    ) from e

# stubgen seems to need this redundant type declaration.
MetricName: type[
    Literal[
        'system.cpu.simple_utilization',
        'system.cpu.time',
        'system.cpu.utilization',
        'system.memory.usage',
        'system.memory.utilization',
        'system.swap.usage',
        'system.swap.utilization',
        'system.disk.io',
        'system.disk.operations',
        'system.disk.time',
        'system.network.dropped.packets',
        'system.network.packets',
        'system.network.errors',
        'system.network.io',
        'system.network.connections',
        'system.thread_count',
        'process.open_file_descriptor.count',
        'process.context_switches',
        'process.cpu.time',
        'process.cpu.utilization',
        'process.cpu.core_utilization',
        'process.memory.usage',
        'process.memory.virtual',
        'process.thread.count',
        'process.runtime.gc_count',
        'cpython.gc.collected_objects',
        'cpython.gc.collections',
        'cpython.gc.uncollectable_objects',
    ]
] = Literal[  # type: ignore  # but pyright doesn't like it
    'system.cpu.simple_utilization',
    'system.cpu.time',
    'system.cpu.utilization',
    'system.memory.usage',
    'system.memory.utilization',
    'system.swap.usage',
    'system.swap.utilization',
    'system.disk.io',
    'system.disk.operations',
    'system.disk.time',
    'system.network.dropped.packets',
    'system.network.packets',
    'system.network.errors',
    'system.network.io',
    'system.network.connections',
    'system.thread_count',
    'process.open_file_descriptor.count',
    'process.context_switches',
    'process.cpu.time',
    'process.cpu.utilization',
    'process.cpu.core_utilization',
    'process.memory.usage',
    'process.memory.virtual',
    'process.thread.count',
    'process.runtime.gc_count',
    'cpython.gc.collected_objects',
    'cpython.gc.collections',
    'cpython.gc.uncollectable_objects',
]

Config = dict[MetricName, Optional[Iterable[str]]]

# All the cpu_times fields provided by psutil (used by system_metrics) across all platforms,
# except for 'guest' and 'guest_nice' which are included in 'user' and 'nice' in Linux (see psutil._cpu_tot_time).
# Docs: https://psutil.readthedocs.io/en/latest/#psutil.cpu_times
CPU_FIELDS: list[LiteralString] = 'idle user system irq softirq nice iowait steal interrupt dpc'.split()

# All the virtual_memory fields provided by psutil across all platforms,
# except for 'percent' which can be calculated as `(total - available) / total * 100`.
# Docs: https://psutil.readthedocs.io/en/latest/#psutil.virtual_memory
MEMORY_FIELDS: list[LiteralString] = 'available used free active inactive buffers cached shared wired slab'.split()

FULL_CONFIG: Config = {
    **cast(Config, _DEFAULT_CONFIG),
    'system.cpu.simple_utilization': None,
    'process.cpu.utilization': None,
    'process.cpu.core_utilization': None,
    'system.cpu.time': CPU_FIELDS,
    'system.cpu.utilization': CPU_FIELDS,
    # For usage, knowing the total amount of bytes available might be handy.
    'system.memory.usage': MEMORY_FIELDS + ['total'],
    # For utilization, the total is always just 1 (100%), so it's not included.
    'system.memory.utilization': MEMORY_FIELDS,
    # The 'free' utilization is not included because it's just 1 - 'used'.
    'system.swap.utilization': ['used'],
}

if sys.platform == 'darwin':  # pragma: no cover
    # see https://github.com/giampaolo/psutil/issues/1219
    # upstream pr: https://github.com/open-telemetry/opentelemetry-python-contrib/pull/2008
    FULL_CONFIG.pop('system.network.connections', None)

for _deprecated in [
    'process.runtime.memory',
    'process.runtime.cpu.time',
    'process.runtime.thread_count',
    'process.runtime.cpu.utilization',
    'process.runtime.context_switches',
]:
    FULL_CONFIG.pop(_deprecated, None)  # type: ignore

BASIC_CONFIG: Config = {
    'process.cpu.utilization': None,
    'system.cpu.simple_utilization': None,
    # The actually used memory ratio can be calculated as `1 - available`.
    'system.memory.utilization': ['available'],
    'system.swap.utilization': ['used'],
}

Base = Literal['basic', 'full', None]


def get_base_config(base: Base) -> Config:
    if base == 'basic':
        return BASIC_CONFIG
    elif base == 'full':
        return FULL_CONFIG
    elif base is None:
        return {}
    else:
        raise ValueError(f'Invalid base: {base}')


def instrument_system_metrics(logfire_instance: Logfire, config: Config | None = None, base: Base = 'basic'):
    config = {**get_base_config(base), **(config or {})}

    if 'system.cpu.simple_utilization' in config:
        measure_simple_cpu_utilization(logfire_instance)

    if 'process.cpu.core_utilization' in config:
        measure_process_cpu_core_utilization(logfire_instance)

    if 'process.runtime.cpu.utilization' in config:  # type: ignore
        # Override OTEL here, see comment in measure_process_runtime_cpu_utilization.<locals>.callback.
        # (The name is also deprecated by OTEL, but that's not really important)
        measure_process_runtime_cpu_utilization(logfire_instance)
        del config['process.runtime.cpu.utilization']  # type: ignore

    if 'process.cpu.utilization' in config:
        # Override OTEL here to avoid emitting 0 in the first measurement.
        measure_process_cpu_utilization(logfire_instance)
        del config['process.cpu.utilization']

    instrumentor = SystemMetricsInstrumentor(config=config)  # type: ignore
    instrumentor.instrument(meter_provider=logfire_instance.config.get_meter_provider())


def measure_simple_cpu_utilization(logfire_instance: Logfire):
    def callback(_options: CallbackOptions) -> Iterable[Observation]:
        # psutil returns a value from 0-100, OTEL values here are generally 0-1, so we divide by 100.
        yield Observation(psutil.cpu_percent() / 100)

    logfire_instance.metric_gauge_callback(
        'system.cpu.simple_utilization',
        [callback],
        description='Average CPU usage across all cores, as a fraction between 0 and 1.',
        unit='1',
    )


def measure_process_runtime_cpu_utilization(logfire_instance: Logfire):
    process = psutil.Process()
    # This first call always returns 0, do it here so that the first real measurement from an exporter
    # will return a nonzero value.
    process.cpu_percent()

    def callback(_options: CallbackOptions) -> Iterable[Observation]:
        # psutil returns a value from 0-100, OTEL values here are generally 0-1, so we divide by 100.
        # OTEL got this wrong: https://github.com/open-telemetry/opentelemetry-python-contrib/issues/2810
        # A fix has been merged there, but we need to know in the dashboard how to interpret the values.
        # So the dashboard will assume a 0-100 range if the scope is 'opentelemetry.instrumentation.system_metrics',
        # and a 0-1 range otherwise. In particular the scope will be 'logfire' if it comes from here.
        yield Observation(process.cpu_percent() / 100)

    logfire_instance.metric_gauge_callback(
        f'process.runtime.{python_implementation().lower()}.cpu.utilization',
        [callback],
        description='Runtime CPU utilization',
        unit='1',
    )


def measure_process_cpu_utilization(logfire_instance: Logfire):
    process = psutil.Process()
    # This first call always returns 0, do it here so that the first real measurement from an exporter
    # will return a nonzero value.
    # Otherwise this function mimics what OTel's SystemMetricsInstrumentor does.
    process.cpu_percent()

    num_cpus = psutil.cpu_count() or 1

    def callback(_options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(process.cpu_percent() / 100 / num_cpus)

    logfire_instance.metric_gauge_callback(
        'process.cpu.utilization',
        [callback],
        description='Runtime CPU utilization',
        unit='1',
    )


def measure_process_cpu_core_utilization(logfire_instance: Logfire):
    """Same as process.cpu.utilization, but not divided by the number of available cores."""
    process = psutil.Process()
    # This first call always returns 0, do it here so that the first real measurement from an exporter
    # will return a nonzero value.
    process.cpu_percent()

    def callback(_options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(process.cpu_percent() / 100)

    logfire_instance.metric_gauge_callback(
        'process.cpu.core_utilization',
        [callback],
        description='Runtime CPU utilization, not divided by the number of available cores.',
        unit='core',
    )
