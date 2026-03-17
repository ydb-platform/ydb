from __future__ import annotations

import sys
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Callable, Literal

from ..constants import ONE_SECOND_IN_NANOSECONDS
from .import_hook import LogfireFinder
from .types import AutoTraceModule

if TYPE_CHECKING:
    from ..main import Logfire


def install_auto_tracing(
    logfire: Logfire,
    modules: Sequence[str] | Callable[[AutoTraceModule], bool],
    *,
    min_duration: float,
    check_imported_modules: Literal['error', 'warn', 'ignore'] = 'error',
) -> None:
    """Install automatic tracing.

    See `Logfire.install_auto_tracing` for more information.
    """
    if isinstance(modules, Sequence):
        modules = modules_func_from_sequence(modules)  # type: ignore

    if not callable(modules):
        raise TypeError('modules must be a list of strings or a callable')

    if check_imported_modules not in ('error', 'warn', 'ignore'):
        raise ValueError('check_imported_modules must be one of "error", "warn", or "ignore"')

    if check_imported_modules != 'ignore':
        for module in list(sys.modules.values()):
            try:
                auto_trace_module = AutoTraceModule(module.__name__, module.__file__)
            except Exception:
                continue

            if modules(auto_trace_module):
                if check_imported_modules == 'error':
                    raise AutoTraceModuleAlreadyImportedException(
                        f'The module {module.__name__!r} matches modules to trace, but it has already been imported. '
                        f'Either call `install_auto_tracing` earlier, '
                        f"or set `check_imported_modules` to 'warn' or 'ignore'."
                    )
                else:
                    warnings.warn(
                        f'The module {module.__name__!r} matches modules to trace, but it has already been imported. '
                        f'Either call `install_auto_tracing` earlier, '
                        f"or set `check_imported_modules` to 'ignore'.",
                        AutoTraceModuleAlreadyImportedWarning,
                        stacklevel=2,
                    )

    min_duration = int(min_duration * ONE_SECOND_IN_NANOSECONDS)
    logfire = logfire.with_settings(custom_scope_suffix='auto_tracing')
    finder = LogfireFinder(logfire, modules, min_duration)
    sys.meta_path.insert(0, finder)


def modules_func_from_sequence(modules: Sequence[str]) -> Callable[[AutoTraceModule], bool]:
    return lambda module: module.parts_start_with(modules)


class AutoTraceModuleAlreadyImportedException(Exception):
    pass


class AutoTraceModuleAlreadyImportedWarning(Warning):
    pass
