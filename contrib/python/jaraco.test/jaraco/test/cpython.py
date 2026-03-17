"""
Compatibility shims for getting stuff from test.support across
Python versions (for compatibility with Python 3.9 and earlier).

>>> os_helper = try_import('os_helper') or from_test_support('temp_dir')
>>> os_helper.temp_dir
<function temp_dir at ...>
"""

from __future__ import annotations

import importlib
import types

from jaraco.collections import Projection
from jaraco.context import suppress


def from_test_support(*names: str) -> types.SimpleNamespace:
    """
    Return a SimpleNamespace of names from test.support.

    >>> support = from_test_support('swap_item')
    >>> support.swap_item
    <function swap_item at ...>
    """
    import test.support

    return types.SimpleNamespace(**Projection(names, vars(test.support)))


@suppress(ImportError)
def try_import(name: str) -> types.ModuleType:
    """
    Attempt to import a submodule of test.support; return None if missing.
    """
    return importlib.import_module(f'test.support.{name}')
