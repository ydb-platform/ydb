"""
fancycompleter: colorful TAB completion for Python prompt
"""

from __future__ import annotations

import contextlib
import os.path
import rlcompleter
import sys
import types
from itertools import count
from typing import Any

try:
    from .version import __version__
except ImportError:
    __version__ = "unknown"


class Color:
    black = "30"
    darkred = "31"
    darkgreen = "32"
    brown = "33"
    darkblue = "34"
    purple = "35"
    teal = "36"
    lightgray = "37"
    darkgray = "30;01"
    red = "31;01"
    green = "32;01"
    yellow = "33;01"
    blue = "34;01"
    fuchsia = "35;01"
    turquoise = "36;01"
    white = "37;01"

    @classmethod
    def set(cls, color: str, string: str):
        with contextlib.suppress(AttributeError):
            color = getattr(cls, color)
        return f"\x1b[{color}m{string}\x1b[00m"


class DefaultConfig:
    consider_getitems = True
    prefer_pyrepl = True
    use_colors: bool | str = "auto"
    readline = None  # set by setup()
    using_pyrepl = False  # overwritten by find_pyrepl

    color_by_type = {
        types.BuiltinMethodType: Color.turquoise,
        types.MethodType: Color.turquoise,
        type((42).__add__): Color.turquoise,
        type(int.__add__): Color.turquoise,
        type(str.replace): Color.turquoise,
        types.FunctionType: Color.blue,
        types.BuiltinFunctionType: Color.blue,
        type: Color.fuchsia,
        types.ModuleType: Color.teal,
        type(None): Color.lightgray,
        str: Color.green,
        int: Color.yellow,
        float: Color.yellow,
        complex: Color.yellow,
        bool: Color.yellow,
    }
    # Fallback to look up colors by `isinstance` when not matched
    # via color_by_type.
    color_by_baseclass = [
        ((BaseException,), Color.red),
    ]

    def find_pyrepl(self):
        if sys.version_info >= (3, 13):
            import _pyrepl.completing_reader
            import _pyrepl.readline

            # readline is not available on windows by default
            if sys.platform == "win32":
                sys.modules["readline"] = _pyrepl.readline

            self.using_pyrepl = True
            return _pyrepl.readline, True

        try:  # type: ignore[unreachable]
            import pyrepl.completing_reader
            import pyrepl.readline
        except ImportError:
            return None

        self.using_pyrepl = True
        if hasattr(pyrepl.completing_reader, "stripcolor"):
            # modern version of pyrepl
            return pyrepl.readline, True
        return pyrepl.readline, False

    def find_pyreadline(self):
        try:
            import readline

            import pyreadline  # noqa: F401  # XXX: needed really?
            from pyreadline.modes import basemode
        except ImportError:
            return None
        if hasattr(basemode, "stripcolor"):
            # modern version of pyreadline; see:
            # https://github.com/pyreadline/pyreadline/pull/48
            return readline, True
        else:
            return readline, False

    def find_best_readline(self):
        if self.prefer_pyrepl:
            result = self.find_pyrepl()
            if result:
                readline, supports_color = result
                if sys.version_info >= (3, 13):
                    readline.backend = "_pyrepl"
                return readline, supports_color
        if sys.platform == "win32":
            result = self.find_pyreadline()
            if result:
                return result
        import readline

        return readline, False  # by default readline does not support colors

    def setup(self):
        self.readline, supports_color = self.find_best_readline()
        if self.use_colors == "auto":
            self.use_colors = supports_color


def execfile(filename: str, namespace: dict[str, Any]):
    with open(filename) as f:
        code = compile(f.read(), filename, "exec")
        exec(code, namespace)


class ConfigurableClass:
    DefaultConfig: type | None = None
    config_filename: str | None = None

    def get_config(self, Config):
        if Config is not None:
            return Config()

        # try to load config from the ~/filename file
        filename = f"~/{self.config_filename}"
        rcfile = os.path.normpath(os.path.expanduser(filename))
        if not os.path.exists(rcfile):
            if self.DefaultConfig is None:
                raise ValueError("DefaultConfig cannot be None")
            return self.DefaultConfig()

        namespace: dict[str, Any] = {}
        try:
            execfile(rcfile, namespace)
        except Exception as exc:
            import traceback

            sys.stderr.write(f"** error when importing {filename}: {exc!r} **\n")
            traceback.print_tb(sys.exc_info()[2])
            if self.DefaultConfig is None:
                raise ValueError("DefaultConfig cannot be None") from None
            return self.DefaultConfig()

        try:
            Config = namespace["Config"]
        except KeyError:
            if self.DefaultConfig is None:
                raise ValueError("DefaultConfig cannot be None") from None
            return self.DefaultConfig()

        try:
            return Config()
        except Exception as exc:
            err = f"error when setting up Config from {filename}: {exc}"
            tb = sys.exc_info()[2]
            if tb and tb.tb_next:
                tb = tb.tb_next
                err_fname = tb.tb_frame.f_code.co_filename
                err_lnum = tb.tb_lineno
                err += f" ({err_fname}:{err_lnum})"
            sys.stderr.write(f"** {err} **\n")

        if self.DefaultConfig is None:
            raise ValueError("DefaultConfig cannot be None")
        return self.DefaultConfig()


class Completer(rlcompleter.Completer, ConfigurableClass):
    """
    When doing something like a.b.<TAB>, display only the attributes of
    b instead of the full a.b.attr string.

    Optionally, display the various completions in different colors
    depending on the type.
    """

    DefaultConfig = DefaultConfig
    config_filename = ".fancycompleterrc.py"

    def __init__(self, namespace: dict | None = None, Config=None):
        self.namespace: dict | None = None
        super().__init__(namespace)
        self.config = self.get_config(Config)
        self.config.setup()
        assert isinstance(self.config.use_colors, bool)

        readline = self.config.readline
        if hasattr(readline, "_setup"):
            # this is needed to offer pyrepl a better chance to patch
            # raw_input. Usually, it does at import time, but is we are under
            # pytest with output captured, at import time we don't have a
            # terminal and thus the raw_input hook is not installed
            if sys.version_info >= (3, 13):
                readline._setup(namespace or {})  # internal _pyrepl implementation
            else:
                readline._setup()

        if self.config.use_colors:
            readline.parse_and_bind("set dont-escape-ctrl-chars on")
        if self.config.consider_getitems:
            delims = readline.get_completer_delims()
            delims = delims.replace("[", "")
            delims = delims.replace("]", "")
            readline.set_completer_delims(delims)

    def complete(self, text: str, state: int):
        # stolen from: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/496812
        if not text:
            return ["\t", None][state]

        return super().complete(text, state)

    def _callable_postfix(self, val, word):
        # disable automatic insertion of '(' for global callables
        return word

    def global_matches(self, text: str) -> list[str]:
        import keyword

        names = super().global_matches(text)
        prefix = commonprefix(names)
        if prefix and prefix != text:
            return [prefix]

        names.sort()
        values: list[Any] = []
        for name in names:
            clean_name = name.rstrip(": ")
            if clean_name in keyword.kwlist:
                values.append(None)
            else:
                try:
                    values.append(eval(name, self.namespace))
                except Exception as exc:
                    values.append(exc)
        if self.config.use_colors and names:
            return self.color_matches(names, values)
        return names

    def attr_matches(self, text: str) -> list[str]:
        expr, attr = text.rsplit(".", 1)
        if "(" in expr or ")" in expr:  # don't call functions
            return []
        try:
            thisobject = eval(expr, self.namespace)
        except Exception:
            return []

        # get the content of the object, except __builtins__
        words = set(dir(thisobject))
        words.discard("__builtins__")

        if hasattr(thisobject, "__class__"):
            words.add("__class__")
            words.update(
                rlcompleter.get_class_members(thisobject.__class__),  # type: ignore[attr-defined]
            )
        names = []
        values = []
        n = len(attr)
        if attr == "":
            noprefix = "_"
        elif attr == "_":
            noprefix = "__"
        else:
            noprefix = None
        while True:
            for word in sorted(words):
                if word[:n] == attr and not (noprefix and word[: n + 1] == noprefix):
                    try:
                        val = getattr(thisobject, word)
                    except Exception:
                        val = None  # Include even if attribute not set

                    names.append(word)
                    values.append(val)
            if names or not noprefix:
                break
            noprefix = "__" if noprefix == "_" else None

        if not names:
            return []

        if len(names) == 1:
            return [f"{expr}.{names[0]}"]  # only option, no coloring.

        prefix = commonprefix(names)
        if prefix and prefix != attr:
            return [f"{expr}.{prefix}"]  # autocomplete prefix

        if self.config.use_colors:
            return self.color_matches(names, values)

        if prefix:
            names.append(" ")
        return names

    def color_matches(self, names: list[str], values: list[Any]):
        matches = [
            self.color_for_obj(i, name, obj)
            for i, name, obj in zip(count(), names, values)
        ]
        # We add a space at the end to prevent the automatic completion of the
        # common prefix, which is the ANSI ESCAPE sequence.
        return matches + [" "]

    def color_for_obj(self, i: int, name: str, value: Any) -> str:
        color = self.config.color_by_type.get(type(value), None)
        if color is None:
            for x, _color in self.config.color_by_baseclass:
                if isinstance(value, x):
                    color = _color
                    break
            else:
                color = "00"
        # hack: prepend an (increasing) fake escape sequence,
        # so that readline can sort the matches correctly.
        return f"\x1b[{i:03d};00m" + Color.set(color, name)


def commonprefix(names: list[str], base: str = ""):
    """return the common prefix of all 'names' starting with 'base'"""
    if base:
        names = [x for x in names if x.startswith(base)]
    if not names:
        return ""
    s1 = min(names)
    s2 = max(names)
    for i, c in enumerate(s1):
        if c != s2[i]:
            return s1[:i]
    return s1


def has_libedit(config) -> bool:
    # https://docs.python.org/3/library/readline.html#readline.backend
    if sys.version_info >= (3, 13) and hasattr(config.readline, "backend"):
        return config.readline.backend == "editline"

    # Detect if we are using libedit/editline.
    # Adapted from IPython's rlineimpl.py.
    if config.using_pyrepl or sys.platform != "darwin":
        return False

    # Official Python docs state that 'libedit' is in the docstring for
    # libedit readline.
    return config.readline.__doc__ and "libedit" in config.readline.__doc__


def setup(namespace: dict | None) -> Completer:
    """Install fancycompleter as the default completer for readline."""
    completer = Completer(namespace)
    readline = completer.config.readline
    if has_libedit(completer.config):
        readline.parse_and_bind("bind ^I rl_complete")
    else:
        readline.parse_and_bind("tab: complete")
    readline.set_completer(completer.complete)
    return completer


def interact_pyrepl(namespace: dict | None = None):
    if sys.version_info >= (3, 13):
        from _pyrepl import readline
        from _pyrepl.console import InteractiveColoredConsole
        from _pyrepl.simple_interact import run_multiline_interactive_console

        console = InteractiveColoredConsole(namespace)
        run_multiline_interactive_console(console)
        sys.modules["readline"] = readline
    else:
        from pyrepl import readline
        from pyrepl.simple_interact import run_multiline_interactive_console

        sys.modules["readline"] = readline

        run_multiline_interactive_console()
    # TODO: restore old readline on exit?


def setup_history(completer, persist_history: str):
    import atexit

    readline = completer.config.readline

    filename = (
        persist_history
        if isinstance(persist_history, str) and persist_history
        else "~/.history.py"
    )

    filename = os.path.expanduser(filename)
    if os.path.isfile(filename):
        readline.read_history_file(filename)

    def save_history():
        readline.write_history_file(filename)

    atexit.register(save_history)


def interact(persist_history: str | None = None, namespace: dict | None = None):
    """
    Main entry point for fancycompleter: run an interactive Python session
    after installing fancycompleter.

    This function is supposed to be called at the end of PYTHONSTARTUP:

      - if we are using pyrepl: install fancycompleter, run pyrepl multiline
        prompt, and sys.exit().  The standard python prompt will never be
        reached

      - if we are not using pyrepl: install fancycompleter and return.  The
        execution will continue as normal, and the standard python prompt will
        be displayed.

    This is necessary because there is no way to tell the standard python
    prompt to use the readline provided by pyrepl instead of the builtin one.

    By default, pyrepl is preferred and automatically used if found.
    """

    if namespace is None:
        import __main__

        namespace = __main__.__dict__

    completer = setup(namespace)
    if persist_history is not None:
        setup_history(completer, persist_history)

    if completer.config.using_pyrepl and "__pypy__" not in sys.builtin_module_names:
        # if we are on PyPy, we don't need to run a "fake" interpreter, as the
        # standard one is fake enough :-)
        interact_pyrepl(namespace)
        sys.exit()
