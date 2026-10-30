# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


from typing import Final

from opentelemetry.metrics import Counter, Meter

CPYTHON_GC_COLLECTED_OBJECTS: Final = "cpython.gc.collected_objects"
"""
The total number of objects collected inside a generation since interpreter start
Instrument: counter
Unit: {object}
Note: This metric reports data from [`gc.stats()`](https://docs.python.org/3/library/gc.html#gc.get_stats).
"""


def create_cpython_gc_collected_objects(meter: Meter) -> Counter:
    """The total number of objects collected inside a generation since interpreter start"""
    return meter.create_counter(
        name=CPYTHON_GC_COLLECTED_OBJECTS,
        description="The total number of objects collected inside a generation since interpreter start.",
        unit="{object}",
    )


CPYTHON_GC_COLLECTIONS: Final = "cpython.gc.collections"
"""
The number of times a generation was collected since interpreter start
Instrument: counter
Unit: {collection}
Note: This metric reports data from [`gc.stats()`](https://docs.python.org/3/library/gc.html#gc.get_stats).
"""


def create_cpython_gc_collections(meter: Meter) -> Counter:
    """The number of times a generation was collected since interpreter start"""
    return meter.create_counter(
        name=CPYTHON_GC_COLLECTIONS,
        description="The number of times a generation was collected since interpreter start.",
        unit="{collection}",
    )


CPYTHON_GC_UNCOLLECTABLE_OBJECTS: Final = "cpython.gc.uncollectable_objects"
"""
The total number of objects which were found to be uncollectable inside a generation since interpreter start
Instrument: counter
Unit: {object}
Note: This metric reports data from [`gc.stats()`](https://docs.python.org/3/library/gc.html#gc.get_stats).
"""


def create_cpython_gc_uncollectable_objects(meter: Meter) -> Counter:
    """The total number of objects which were found to be uncollectable inside a generation since interpreter start"""
    return meter.create_counter(
        name=CPYTHON_GC_UNCOLLECTABLE_OBJECTS,
        description="The total number of objects which were found to be uncollectable inside a generation since interpreter start.",
        unit="{object}",
    )
