# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""
Caching and compatibility wrapper for standard library ``importlib.metadata``.

This module caches ``entry_points()`` to avoid reloading entry points from disk on every call.
It also normalizes minor differences across python versions 3.10+. References to issues:

- https://github.com/open-telemetry/opentelemetry-python/pull/4735
- https://github.com/open-telemetry/opentelemetry-python/pull/5203
"""

import itertools
from functools import cache
from importlib.metadata import (
    Distribution,
    EntryPoint,
    EntryPoints,
    PackageNotFoundError,
    distributions,
    requires,
    version,
)
from importlib.metadata import entry_points as original_entry_points
from typing import Any


def _as_entry_points(eps: Any) -> EntryPoints:
    # Python versions greater than 3.11 return EntryPoints.
    if isinstance(eps, EntryPoints):
        return eps
    # In Python 3.10 and 3.11 entry_points() returns
    # a dict-like SelectableGroups object.
    # Use dict.values() instead of eps.values() to avoid the DeprecationWarning
    # that SelectableGroups raises when calling .values().
    if isinstance(eps, dict):
        return EntryPoints(itertools.chain.from_iterable(dict.values(eps)))
    # This case should be unreachable, but is included as a fallback.
    return EntryPoints(
        ep for group in eps.groups for ep in eps.select(group=group)
    )


@cache
def _original_entry_points_cached() -> EntryPoints:
    return _as_entry_points(original_entry_points())


def entry_points(**params) -> EntryPoints:
    return _original_entry_points_cached().select(**params)


__all__ = [
    "entry_points",
    "version",
    "EntryPoint",
    "EntryPoints",
    "requires",
    "Distribution",
    "distributions",
    "PackageNotFoundError",
]
