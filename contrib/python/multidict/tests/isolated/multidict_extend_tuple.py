import gc
import sys
from typing import Any

import objgraph  # type: ignore[import-untyped]

from multidict import MultiDict


class NotLeakTuple(tuple[Any, ...]):
    """A subclassed tuple to make it easier to test for leaks."""


def _run_isolated_case() -> None:
    md: MultiDict[str] = MultiDict()
    for _ in range(100):
        md.extend(NotLeakTuple())
    del md
    gc.collect()

    leaked = len(objgraph.by_type("NotLeakTuple"))
    print(f"{leaked} instances of NotLeakTuple not collected by GC")
    sys.exit(1 if leaked else 0)


if __name__ == "__main__":
    _run_isolated_case()
