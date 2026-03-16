#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import Callable, Generic, TypeVar

T = TypeVar("T")


class LazyVar(Generic[T]):
    """Our implementation of a lazy variable.

    Mostly used for when we want to implement a shared variable between tests (should be calculated at most once),
    but only if necessary.
    """

    def __init__(self, generator: Callable[[], T]):
        """Initializes a lazy variable.

        Args:
            generator: A function that takes no arguments and generates the actual variable.
        """
        self.value = None
        self.generator = generator

    def get(self) -> T:
        if self.value is None:
            self.value = self.generator()
        return self.value
