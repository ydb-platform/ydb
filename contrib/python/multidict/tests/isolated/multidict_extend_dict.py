import gc
import sys
from typing import Any

import objgraph  # type: ignore[import-untyped]

from multidict import MultiDict


class NoLeakDict(dict[str, Any]):
    """A subclassed dict to make it easier to test for leaks."""


def _run_isolated_case() -> None:
    md: MultiDict[str] = MultiDict()
    for _ in range(100):
        md.update(NoLeakDict())
    del md
    gc.collect()

    leaked = len(objgraph.by_type("NoLeakDict"))
    print(f"{leaked} instances of NoLeakDict not collected by GC")
    sys.exit(1 if leaked else 0)


if __name__ == "__main__":
    _run_isolated_case()
