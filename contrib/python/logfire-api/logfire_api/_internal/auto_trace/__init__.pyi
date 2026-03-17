from ..constants import ONE_SECOND_IN_NANOSECONDS as ONE_SECOND_IN_NANOSECONDS
from ..main import Logfire as Logfire
from .import_hook import LogfireFinder as LogfireFinder
from .types import AutoTraceModule as AutoTraceModule
from collections.abc import Sequence
from typing import Callable, Literal

def install_auto_tracing(logfire: Logfire, modules: Sequence[str] | Callable[[AutoTraceModule], bool], *, min_duration: float, check_imported_modules: Literal['error', 'warn', 'ignore'] = 'error') -> None:
    """Install automatic tracing.

    See `Logfire.install_auto_tracing` for more information.
    """
def modules_func_from_sequence(modules: Sequence[str]) -> Callable[[AutoTraceModule], bool]: ...

class AutoTraceModuleAlreadyImportedException(Exception): ...
class AutoTraceModuleAlreadyImportedWarning(Warning): ...
