import click
from click.decorators import _param_memo

from cloup._util import click_version_ge_8_2


def make_arg_metavar(arg, ctx) -> str:
    if click_version_ge_8_2:
        return arg.make_metavar(ctx)  # type: ignore[call-arg]
    return arg.make_metavar()  # type: ignore[call-arg]


class Argument(click.Argument):
    """A :class:`click.Argument` with help text."""

    def __init__(self, *args, help=None, **attrs):
        super().__init__(*args, **attrs)
        self.help = help

    def get_help_record(self, ctx):
        return make_arg_metavar(self, ctx), self.help or ""


class Option(click.Option):
    """A :class:`click.Option` with an extra field ``group`` of type ``OptionGroup``."""

    def __init__(self, *args, group=None, **attrs):
        super().__init__(*args, **attrs)
        self.group = group


GroupedOption = Option
"""Alias of ``Option``."""


def argument(*param_decls, cls=None, **attrs):
    ArgumentClass = cls or Argument

    def decorator(f):
        _param_memo(f, ArgumentClass(param_decls, **attrs))
        return f

    return decorator


def option(*param_decls, cls=None, group=None, **attrs):
    """Attach an ``Option`` to the command.
    Refer to :class:`click.Option` and :class:`click.Parameter` for more info
    about the accepted parameters.

    In your IDE, you won't see arguments relating to shell completion,
    because they are different in Click 7 and 8 (both supported by Cloup):

    - in Click 7, it's ``autocompletion``
    - in Click 8, it's ``shell_complete``.

    These arguments have different semantics, refer to Click's docs.
    """
    OptionClass = cls or Option

    def decorator(f):
        _param_memo(f, OptionClass(param_decls, **attrs))
        new_option = f.__click_params__[-1]
        new_option.group = group
        if group and group.hidden:
            new_option.hidden = True
        return f

    return decorator
