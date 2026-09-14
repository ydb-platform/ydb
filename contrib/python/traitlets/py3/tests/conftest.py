from __future__ import annotations

import sys

collect_ignore = []

# mypy does not support PyPy (https://github.com/python/mypy/issues/20329), so
# neither mypy nor pytest-mypy-testing is installed there and the mypy_*_typing
# checks cannot run.
if sys.implementation.name == "pypy":
    collect_ignore.append("test_typing.py")
