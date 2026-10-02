import enum
import rich.console


class VerbosityMode(enum.Enum):
    QUIET = 0
    NORMAL = 1
    VERBOSE = 2


_state = {
    "mode": VerbosityMode.NORMAL,
    "console": None,
    "stderr_console": None,
    "active_progress": None,
    "progress_backend_override": None,
}


def init(mode: VerbosityMode = VerbosityMode.NORMAL) -> None:
    """Initialize output management. Idempotent - creates consoles only on first call."""
    if _state["console"] is None:
        _state["mode"] = mode
        _state["console"] = rich.console.Console()
        _state["stderr_console"] = rich.console.Console(stderr=True)


def get_mode() -> VerbosityMode:
    return _state["mode"]


def get_console() -> rich.console.Console:
    if _state["console"] is None:
        raise RuntimeError("output.init() must be called before get_console()")
    return _state["console"]


def get_stderr_console() -> rich.console.Console:
    if _state["stderr_console"] is None:
        raise RuntimeError("output.init() must be called before get_stderr_console()")
    return _state["stderr_console"]


def _set_active_progress(progress) -> None:
    """Register or unregister the currently active MyProgress instance.

    Called by MyProgress.__enter__ / __exit__ so that logging_setup can
    dynamically route RichHandler output through the progress console,
    preventing log lines from breaking the live progress display.
    """
    _state["active_progress"] = progress
    # Notify logging_setup to switch handler target
    try:
        from ydb.tools.mnc.lib import logging_setup
        logging_setup.update_handler_console()
    except Exception:
        pass


def get_active_progress():
    """Return the currently active MyProgress instance, or None."""
    return _state["active_progress"]


def set_progress_backend_override(backend) -> None:
    _state["progress_backend_override"] = backend


def get_progress_backend_override():
    return _state.get("progress_backend_override")
