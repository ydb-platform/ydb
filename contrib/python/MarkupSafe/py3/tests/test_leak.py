from __future__ import annotations

import gc

from markupsafe import escape


def test_markup_leaks() -> None:
    counts = set()
    # Try to start with a "clean" count. Works for PyPy but not 3.13 JIT.
    gc.collect()

    for _ in range(20):
        for _ in range(1000):
            escape("foo")
            escape("<foo>")
            escape("foo")
            escape("<foo>")

        counts.add(len(gc.get_objects()))

    # Some implementations, such as PyPy and Python 3.13 JIT, end up with 2
    # counts rather than one. Presumably this is internals stabilizing. A leak
    # would presumably have a different count every loop.
    assert len(counts) < 3
