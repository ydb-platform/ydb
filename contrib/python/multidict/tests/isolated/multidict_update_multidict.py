import gc
import sys

import objgraph  # type: ignore[import-untyped]

from multidict import MultiDict


def _run_isolated_case() -> None:
    md: MultiDict[str] = MultiDict()
    for _ in range(100):
        md.extend(MultiDict())
    del md
    gc.collect()
    leaked = len(objgraph.by_type("MultiDict"))
    print(f"{leaked} instances of MultiDict not collected by GC")
    sys.exit(1 if leaked else 0)


if __name__ == "__main__":
    _run_isolated_case()
