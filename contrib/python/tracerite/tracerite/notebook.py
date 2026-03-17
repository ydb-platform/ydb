from __future__ import annotations

import contextlib
import sys
from typing import Any

from . import trace
from .html import html_traceback
from .logging import logger
from .tty import tty_traceback

# Cleanup mode: "replace" (default) removes old reports, "keep" only removes script/style
_cleanup_mode = "replace"


def _can_display_html() -> bool:
    # Spyder runs IPython ZMQInteractiveShell but lacks HTML support. Using
    # argv seems like the most portable way to autodetect HTML capability.
    #
    # "ipykernel_launcher.py" in Jupyter Notebook/Lab
    # "ipykernel/__main__.py" in Azure Notebooks
    # "colab_kernel_launcher.py" in Google Colab
    return any(name in sys.argv[0] for name in ["ipykernel", "colab_kernel_launcher"])


def load_ipython_extension(ipython: Any) -> None:
    trace.ipython = ipython

    # Hide IPython's internal frames from tracebacks
    try:
        from IPython.core import interactiveshell  # type: ignore[import]

        interactiveshell.__tracebackhide__ = True  # type: ignore[attr-defined]
    except ImportError:
        pass

    # Define handlers that check HTML capability at call time, not load time.
    # This allows the same extension to work in both Jupyter and terminal.
    def showtraceback(*args: Any, **kwargs: Any) -> None:
        try:
            if _can_display_html():
                from IPython.display import display  # type: ignore[import]

                display(
                    html_traceback(
                        skip_until="<ipython-input-",
                        replace_previous=True,
                        cleanup_mode=_cleanup_mode,
                        autodark=False,
                    )
                )
            else:
                tty_traceback(skip_until="<ipython-input-")
        except Exception:
            # Fall back to built-in showtraceback
            ipython.__class__.showtraceback(ipython, *args, **kwargs)

    def showsyntaxerror(*args: Any, **kwargs: Any) -> None:
        try:
            if _can_display_html():
                from IPython.display import display  # type: ignore[import]

                display(
                    html_traceback(
                        skip_until="<ipython-input-",
                        replace_previous=True,
                        cleanup_mode=_cleanup_mode,
                        autodark=False,
                    )
                )
            else:
                tty_traceback(skip_until="<ipython-input-")
        except Exception:
            # Fall back to built-in showsyntaxerror
            ipython.__class__.showsyntaxerror(ipython, *args, **kwargs)

    # Install the handlers
    try:
        ipython.showtraceback = showtraceback
        ipython.showsyntaxerror = showsyntaxerror
    except Exception:
        logger.error("Unable to load TraceRite (please report a bug!)")
        raise

    # Register the %tracerite magic command
    from IPython.core.magic import register_line_magic  # type: ignore[import]

    @register_line_magic
    def tracerite(line: str) -> None:
        """Configure tracerite behavior.

        Usage:
            %tracerite keep - Keep all previous error reports visible
        """
        global _cleanup_mode
        if line.strip().lower() == "keep":
            _cleanup_mode = "keep"
        else:
            print("Usage: %tracerite keep")


def unload_ipython_extension(ipython: Any) -> None:
    with contextlib.suppress(AttributeError):
        del ipython.showtraceback
    with contextlib.suppress(AttributeError):
        del ipython.showsyntaxerror
    # Remove the __tracebackhide__ we injected
    try:
        from IPython.core import interactiveshell  # type: ignore[import]

        with contextlib.suppress(AttributeError):
            del interactiveshell.__tracebackhide__  # type: ignore[attr-defined]
    except ImportError:
        pass
    trace.ipython = None
