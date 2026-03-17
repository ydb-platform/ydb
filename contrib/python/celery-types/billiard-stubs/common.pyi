import pickle
from signal import _HANDLER, _SIGNUM, Signals

from billiard.exceptions import RestartFreqExceeded as _RestartFreqExceeded

pickle_load = pickle.load
pickle_loads = pickle.loads
SIGMAP: dict[Signals, str]
TERM_SIGNAL: Signals
TERM_SIGNAME: str
REMAP_SIGTERM: str | None
TERMSIGS_IGNORE: set[str]
TERMSIGS_FORCE: set[str]
EX_SOFTWARE: int
TERMSIGS_DEFAULT: set[str]
TERMSIGS_FULL: set[str]

def human_status(status: int) -> str: ...
def maybe_setsignal(signum: _SIGNUM, handler: _HANDLER) -> None: ...
def signum(sig: int) -> _SIGNUM: ...
def reset_signals(handler: _HANDLER = ..., full: bool = ...) -> None: ...

class restart_state:
    RestartFreqExceeded = _RestartFreqExceeded
    def __init__(self, maxR: int, maxT: int) -> None: ...
    R: int
    T: float | None
    def step(self, now: float | None = ...) -> None: ...
