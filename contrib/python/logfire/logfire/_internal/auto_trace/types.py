from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass
class AutoTraceModule:
    """Information about a module being imported that should maybe be traced automatically.

    This object will be passed to a function that should return True if the module should be traced.
    In particular it'll be passed to a function that's passed to `install_auto_tracing` as the `modules` argument.
    """

    name: str
    """Fully qualified absolute name of the module being imported."""

    filename: str | None
    """Filename of the module being imported."""

    # The argument is meant to match `str.startswith`.
    # The method name is different to avoid confusion with `.name.startswith` which behaves slightly differently.
    def parts_start_with(self, prefix: str | Sequence[str]) -> bool:
        """Return True if the module name starts with any of the given prefixes, using dots as boundaries.

        For example, if the module name is `foo.bar.spam`, then `parts_start_with('foo')` will return True,
        but `parts_start_with('bar')` or `parts_start_with('foo_bar')` will return False.
        In other words, this will match the module itself or any submodules.

        If a prefix contains any characters other than letters, numbers, and dots,
        then it will be treated as a regular expression.
        """
        if isinstance(prefix, str):
            prefix = (prefix,)
        pattern = '|'.join([get_module_pattern(p) for p in prefix])
        return bool(re.match(pattern, self.name))


def get_module_pattern(module: str):
    if not re.match(r'[\w.]+$', module, re.UNICODE):
        return module  # treat as regex
    module = re.escape(module)  # escape dots
    return rf'{module}($|\.)'
