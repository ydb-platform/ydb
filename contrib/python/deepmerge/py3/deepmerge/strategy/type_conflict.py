from __future__ import annotations

from collections.abc import Sized
from typing import Any, TypeVar

import deepmerge.merger
from .core import StrategyList

T1 = TypeVar("T1")
T2 = TypeVar("T2")


class TypeConflictStrategies(StrategyList):
    """contains the strategies provided for type conflicts."""

    NAME = "type conflict"

    @staticmethod
    def strategy_override(config: deepmerge.merger.Merger, path: list, base: Any, nxt: T1) -> T1:
        """overrides the new object over the old object"""
        return nxt

    @staticmethod
    def strategy_use_existing(
        config: deepmerge.merger.Merger, path: list, base: T1, nxt: Any
    ) -> T1:
        """uses the old object instead of the new object"""
        return base

    @staticmethod
    def strategy_override_if_not_empty(
        config: deepmerge.merger.Merger, path: list, base: T1, nxt: T2
    ) -> T1 | T2:
        """overrides the new object over the old object only if the new object is not empty or null.

        ``None`` is treated as null.  Sized objects (``dict``, ``list``, ``set``, ``str``, …) are
        treated as empty when ``len(nxt) == 0``.  All other values — including falsy primitives such
        as ``0``, ``0.0``, and ``False`` — are considered non-empty and will override *base*.
        """
        if nxt is None:
            return base
        if isinstance(nxt, Sized) and len(nxt) == 0:
            return base
        return nxt
