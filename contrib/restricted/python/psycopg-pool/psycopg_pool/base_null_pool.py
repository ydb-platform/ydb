"""
Psycopg mixin class for null connection pools
"""

# Copyright (C) 2022 The Psycopg Team

from __future__ import annotations


class _BaseNullConnectionPool:
    def _check_size(self, min_size: int, max_size: int | None) -> tuple[int, int]:
        if max_size is None:
            max_size = min_size

        if min_size != 0:
            raise ValueError("null pools must have min_size = 0")
        if max_size < min_size:
            raise ValueError("max_size must be greater or equal than min_size")

        return min_size, max_size

    def _start_initial_tasks(self) -> None:
        # Null pools don't have background tasks to fill connections
        # or to grow/shrink.
        pass

    def _maybe_grow_pool(self) -> None:
        # null pools don't grow
        pass
