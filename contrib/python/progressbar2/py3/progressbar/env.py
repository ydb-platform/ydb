from __future__ import annotations

import contextlib
import enum
import os
import re
import typing


@typing.overload
def env_flag(name: str, default: bool) -> bool: ...


@typing.overload
def env_flag(name: str, default: bool | None = None) -> bool | None: ...


def env_flag(name: str, default: bool | None = None) -> bool | None:
    """
    Accepts environt variables formatted as y/n, yes/no, 1/0, true/false,
    on/off, and returns it as a boolean.

    If the environment variable is not defined, or has an unknown value,
    returns `default`
    """
    v = os.getenv(name)
    if v and v.lower() in ('y', 'yes', 't', 'true', 'on', '1'):
        return True
    if v and v.lower() in ('n', 'no', 'f', 'false', 'off', '0'):
        return False
    return default


class ColorSupport(enum.IntEnum):
    """Color support for the terminal."""

    NONE = 0
    XTERM = 16
    XTERM_256 = 256
    XTERM_TRUECOLOR = 16777216
    WINDOWS = 8

    @classmethod
    def from_env(cls) -> ColorSupport:
        """Get the color support from the environment.

        If any of the environment variables contain `24bit` or `truecolor`,
        we will enable true color/24 bit support. If they contain `256`, we
        will enable 256 color/8 bit support. If they contain `xterm`, we will
        enable 16 color support. Otherwise, we will assume no color support.

        If `JUPYTER_COLUMNS` or `JUPYTER_LINES` or `JPY_PARENT_PID` is set, we
        will assume true color support.

        Note that the highest available value will be used! Having
        `COLORTERM=truecolor` will override `TERM=xterm-256color`.
        """
        variables = (
            'FORCE_COLOR',
            'PROGRESSBAR_ENABLE_COLORS',
            'COLORTERM',
            'TERM',
        )

        if JUPYTER:
            # Jupyter notebook always supports true color.
            return cls.XTERM_TRUECOLOR
        elif os.name == 'nt':
            # We can't reliably detect true color support on Windows, so we
            # will assume it is supported if the console is configured to
            # support it.
            from .terminal.os_specific import windows

            if (
                windows.get_console_mode()
                & windows.WindowsConsoleModeFlags.ENABLE_PROCESSED_OUTPUT
            ):
                return cls.XTERM_TRUECOLOR
            else:
                return cls.WINDOWS  # pragma: no cover

        support = cls.NONE
        for variable in variables:
            value = os.environ.get(variable)
            if value is None:
                continue
            elif value in {'truecolor', '24bit'}:
                # Truecolor support, we don't need to check anything else.
                support = cls.XTERM_TRUECOLOR
                break
            elif '256' in value:
                support = max(cls.XTERM_256, support)
            elif value == 'xterm':
                support = max(cls.XTERM, support)

        return support


def is_ansi_terminal(
    fd: typing.IO[typing.Any],
    is_terminal: bool | None = None,
) -> bool | None:  # pragma: no cover
    if is_terminal is None:
        # Jupyter Notebooks support progress bars
        if JUPYTER:
            is_terminal = True
        # This works for newer versions of pycharm only. With older versions
        # there is no way to check.
        elif os.environ.get('PYCHARM_HOSTED') == '1' and not os.environ.get(
            'PYTEST_CURRENT_TEST'
        ):
            is_terminal = True

    if is_terminal is None:
        # check if we are writing to a terminal or not. typically a file object
        # is going to return False if the instance has been overridden and
        # isatty has not been defined we have no way of knowing so we will not
        # use ansi.  ansi terminals will typically define one of the 2
        # environment variables.
        with contextlib.suppress(Exception):
            is_tty: bool = fd.isatty()
            # Try and match any of the huge amount of Linux/Unix ANSI consoles
            if is_tty and ANSI_TERM_RE.match(os.environ.get('TERM', '')):
                is_terminal = True
            # ANSICON is a Windows ANSI compatible console
            elif 'ANSICON' in os.environ:
                is_terminal = True
            elif os.name == 'nt':
                from .terminal.os_specific import windows

                return bool(
                    windows.get_console_mode()
                    & windows.WindowsConsoleModeFlags.ENABLE_PROCESSED_OUTPUT,
                )
            else:
                is_terminal = None

    return is_terminal


def is_terminal(
    fd: typing.IO[typing.Any],
    is_terminal: bool | None = None,
) -> bool | None:
    if is_terminal is None:
        # Full ansi support encompasses what we expect from a terminal
        is_terminal = is_ansi_terminal(fd) or None

    if is_terminal is None:
        # Allow a environment variable override
        is_terminal = env_flag('PROGRESSBAR_IS_TERMINAL', None)

    if is_terminal is None:  # pragma: no cover
        # Bare except because a lot can go wrong on different systems. If we do
        # get a TTY we know this is a valid terminal
        try:
            is_terminal = fd.isatty()
        except Exception:
            is_terminal = False

    return is_terminal


# Enable Windows full color mode if possible
if os.name == 'nt':
    pass

    # os_specific.set_console_mode()

JUPYTER = bool(
    os.environ.get('JUPYTER_COLUMNS')
    or os.environ.get('JUPYTER_LINES')
    or os.environ.get('JPY_PARENT_PID')
)
COLOR_SUPPORT = ColorSupport.from_env()
ANSI_TERMS = (
    '([xe]|bv)term',
    '(sco)?ansi',
    'cygwin',
    'konsole',
    'linux',
    'rxvt',
    'screen',
    'tmux',
    'vt(10[02]|220|320)',
)
ANSI_TERM_RE: re.Pattern[str] = re.compile(
    f"^({'|'.join(ANSI_TERMS)})", re.IGNORECASE
)
