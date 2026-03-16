from __future__ import annotations

import warnings
from functools import update_wrapper
from typing import (
    Any, Callable, cast, Dict, List, Optional, Type, TypeVar, TYPE_CHECKING, overload,
)

import click

import cloup
from cloup._util import coalesce, pick_non_missing
from cloup.formatting import HelpFormatter
from cloup.typing import MISSING, Possibly

if TYPE_CHECKING:
    from typing_extensions import Concatenate, ParamSpec

    P = ParamSpec("P")

R = TypeVar("R")


@overload
def get_current_context() -> "Context":
    ...


@overload
def get_current_context(silent: bool = False) -> "Optional[Context]":
    ...


def get_current_context(silent: bool = False) -> "Optional[Context]":
    """Equivalent to :func:`click.get_current_context` but casts the returned
    :class:`click.Context` object to :class:`cloup.Context` (which is safe when using
    cloup commands classes and decorators)."""
    return cast(Optional[Context], click.get_current_context(silent=silent))


def pass_context(f: "Callable[Concatenate[Context, P], R]") -> "Callable[P, R]":
    """Marks a callback as wanting to receive the current context object as first
    argument. Equivalent to :func:`click.pass_context` but assumes the current context
    is of type :class:`cloup.Context`."""

    def new_func(*args: "P.args", **kwargs: "P.kwargs") -> R:
        return f(get_current_context(), *args, **kwargs)

    return update_wrapper(new_func, f)


def _warn_if_formatter_settings_conflict(
    ctx_key: str,
    formatter_key: str,
    ctx_kwargs: Dict[str, Any],
    formatter_settings: Dict[str, Any],
) -> None:
    if ctx_kwargs.get(ctx_key) and formatter_settings.get(formatter_key):
        from textwrap import dedent
        formatter_arg = f'formatter_settings.{formatter_key}'
        warnings.warn(dedent(f"""
        You provided both {ctx_key} and {formatter_arg} as arguments of a Context.
        Unless you have a particular reason, you should set only one of them..
        If you use both, {formatter_arg} will be used by the formatter.
        You can suppress this warning by setting:

            cloup.warnings.formatter_settings_conflict = False
        """))


class Context(click.Context):
    """A custom context for Cloup.

    Look up :class:`click.Context` for the list of all arguments.

    .. versionadded:: 0.9.0
        added the ``check_constraints_consistency`` parameter.

    .. versionadded:: 0.8.0

    :param ctx_args:
        arguments forwarded to :class:`click.Context`.
    :param align_option_groups:
        if True, align the definition lists of all option groups of a command.
        You can override this by setting the corresponding argument of ``Command``
        (but you probably shouldn't: be consistent).
    :param align_sections:
        if True, align the definition lists of all subcommands of a group.
        You can override this by setting the corresponding argument of ``Group``
        (but you probably shouldn't: be consistent).
    :param show_subcommand_aliases:
        whether to show the aliases of subcommands in the help of a ``cloup.Group``.
    :param show_constraints:
        whether to include a "Constraint" section in the command help (if at
        least one constraint is defined).
    :param check_constraints_consistency:
        enable additional checks for constraints which detects mistakes of the
        developer (see :meth:`cloup.Constraint.check_consistency`).
    :param formatter_settings:
        keyword arguments forwarded to :class:`HelpFormatter` in ``make_formatter``.
        This args are merged with those of the (eventual) parent context and then
        merged again (being overridden) by those of the command.
        **Tip**: use the static method :meth:`HelpFormatter.settings` to create this
        dictionary, so that you can be guided by your IDE.
    :param ctx_kwargs:
        keyword arguments forwarded to :class:`click.Context`.
    """
    formatter_class: Type[HelpFormatter] = HelpFormatter

    def __init__(
        self, *ctx_args: Any,
        align_option_groups: Optional[bool] = None,
        align_sections: Optional[bool] = None,
        show_subcommand_aliases: Optional[bool] = None,
        show_constraints: Optional[bool] = None,
        check_constraints_consistency: Optional[bool] = None,
        formatter_settings: Dict[str, Any] = {},
        **ctx_kwargs: Any,
    ):
        super().__init__(*ctx_args, **ctx_kwargs)

        self.align_option_groups = coalesce(
            align_option_groups,
            getattr(self.parent, 'align_option_groups', None),
        )
        self.align_sections = coalesce(
            align_sections,
            getattr(self.parent, 'align_sections', None),
        )
        self.show_subcommand_aliases = coalesce(
            show_subcommand_aliases,
            getattr(self.parent, 'show_subcommand_aliases', None),
        )
        self.show_constraints = coalesce(
            show_constraints,
            getattr(self.parent, 'show_constraints', None),
        )
        self.check_constraints_consistency = coalesce(
            check_constraints_consistency,
            getattr(self.parent, 'check_constraints_consistency', None)
        )

        if cloup.warnings.formatter_settings_conflict:
            _warn_if_formatter_settings_conflict(
                'terminal_width', 'width', ctx_kwargs, formatter_settings)
            _warn_if_formatter_settings_conflict(
                'max_content_width', 'max_width', ctx_kwargs, formatter_settings)

        #: Keyword arguments for the HelpFormatter. Obtained by merging the options
        #: of the parent context with the one passed to this context. Before creating
        #: the help formatter, these options are merged with the (eventual) options
        #: provided to the command (having higher priority).
        self.formatter_settings = {
            **getattr(self.parent, 'formatter_settings', {}),
            **formatter_settings,
        }

    def get_formatter_settings(self) -> Dict[str, Any]:
        return {
            'width': self.terminal_width,
            'max_width': self.max_content_width,
            **self.formatter_settings,
            **getattr(self.command, 'formatter_settings', {})
        }

    def make_formatter(self) -> HelpFormatter:
        opts = self.get_formatter_settings()
        return self.formatter_class(**opts)

    @staticmethod
    def settings(
        *, auto_envvar_prefix: Possibly[str] = MISSING,
        default_map: Possibly[Dict[str, Any]] = MISSING,
        terminal_width: Possibly[int] = MISSING,
        max_content_width: Possibly[int] = MISSING,
        resilient_parsing: Possibly[bool] = MISSING,
        allow_extra_args: Possibly[bool] = MISSING,
        allow_interspersed_args: Possibly[bool] = MISSING,
        ignore_unknown_options: Possibly[bool] = MISSING,
        help_option_names: Possibly[List[str]] = MISSING,
        token_normalize_func: Possibly[Callable[[str], str]] = MISSING,
        color: Possibly[bool] = MISSING,
        show_default: Possibly[bool] = MISSING,
        align_option_groups: Possibly[bool] = MISSING,
        align_sections: Possibly[bool] = MISSING,
        show_subcommand_aliases: Possibly[bool] = MISSING,
        show_constraints: Possibly[bool] = MISSING,
        check_constraints_consistency: Possibly[bool] = MISSING,
        formatter_settings: Possibly[Dict[str, Any]] = MISSING,
    ) -> Dict[str, Any]:
        """Utility method for creating a ``context_settings`` dictionary.

        :param auto_envvar_prefix:
            the prefix to use for automatic environment variables. If this is
            `None` then reading from environment variables is disabled. This
            does not affect manually set environment variables which are always
            read.
        :param default_map:
            a dictionary (like object) with default values for parameters.
        :param terminal_width:
            the width of the terminal. The default is inherited from parent
            context. If no context defines the terminal width then auto-detection
            will be applied.
        :param max_content_width:
            the maximum width for content rendered by Click (this currently
            only affects help pages). This defaults to 80 characters if not
            overridden. In other words: even if the terminal is larger than
            that, Click will not format things wider than 80 characters by
            default.  In addition to that, formatters might add some safety
            mapping on the right.
        :param resilient_parsing:
            if this flag is enabled then Click will parse without any
            interactivity or callback invocation. Default values will also be
            ignored. This is useful for implementing things such as completion
            support.
        :param allow_extra_args:
            if this is set to `True` then extra arguments at the end will not
            raise an error and will be kept on the context. The default is to
            inherit from the command.
        :param allow_interspersed_args:
            if this is set to `False` then options and arguments cannot be
            mixed.  The default is to inherit from the command.
        :param ignore_unknown_options:
            instructs click to ignore options it does not know and keeps them
            for later processing.
        :param help_option_names:
            optionally a list of strings that define how the default help
            parameter is named. The default is ``['--help']``.
        :param token_normalize_func:
            an optional function that is used to normalize tokens (options,
            choices, etc.). This for instance can be used to implement
            case-insensitive behavior.
        :param color:
            controls if the terminal supports ANSI colors or not. The default
            is auto-detection. This is only needed if ANSI codes are used in
            texts that Click prints which is by default not the case. This for
            instance would affect help output.
        :param show_default: Show defaults for all options. If not set,
            defaults to the value from a parent context. Overrides an
            option's ``show_default`` argument.
        :param align_option_groups:
            if True, align the definition lists of all option groups of a command.
            You can override this by setting the corresponding argument of ``Command``
            (but you probably shouldn't: be consistent).
        :param align_sections:
            if True, align the definition lists of all subcommands of a group.
            You can override this by setting the corresponding argument of ``Group``
            (but you probably shouldn't: be consistent).
        :param show_subcommand_aliases:
            whether to show the aliases of subcommands in the help of a ``cloup.Group``.
        :param show_constraints:
            whether to include a "Constraint" section in the command help (if at
            least one constraint is defined).
        :param check_constraints_consistency:
            enable additional checks for constraints which detects mistakes of the
            developer (see :meth:`cloup.Constraint.check_consistency`).
        :param formatter_settings:
            keyword arguments forwarded to :class:`HelpFormatter` in ``make_formatter``.
            This args are merged with those of the (eventual) parent context and then
            merged again (being overridden) by those of the command.
            **Tip**: use the static method :meth:`HelpFormatter.settings` to create this
            dictionary, so that you can be guided by your IDE.
        """
        return pick_non_missing(locals())
