"""
Implements the "option groups" feature.
"""
from collections import defaultdict
from typing import (
    Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, overload,
)

import click
from click import Option, Parameter

import cloup
from cloup._params import option, make_arg_metavar
from cloup._util import first_bool, make_repr
from cloup.constraints import Constraint
from cloup.formatting import HelpSection, ensure_is_cloup_formatter
from cloup.typing import Decorator, F


class OptionGroup:
    def __init__(self, title: str,
                 help: Optional[str] = None,
                 constraint: Optional[Constraint] = None,
                 hidden: bool = False):
        """Contains the information of an option group and identifies it.
        Note that, as far as the clients of this library are concerned, an
        ``OptionGroups`` acts as a "marker" for options, not as a container for
        related options. When you call ``@optgroup.option(...)`` you are not
        adding an option to a container, you are just adding an option marked
        with this option group.

        .. versionadded:: 0.8.0
            The ``hidden`` parameter.
        """
        if not title:
            raise ValueError('name is a mandatory argument')  # pragma: no cover
        self.title = title
        self.help = help
        self._options: Sequence[click.Option] = []
        self.constraint = constraint
        self.hidden = hidden

    @property
    def options(self) -> Sequence[click.Option]:
        return self._options

    @options.setter
    def options(self, options: Iterable[click.Option]) -> None:
        self._options = opts = tuple(options)
        if self.hidden:
            for opt in opts:
                opt.hidden = True
        elif all(opt.hidden for opt in opts):
            self.hidden = True

    def get_help_records(self, ctx: click.Context) -> List[Tuple[str, str]]:
        if self.hidden:
            return []
        return [
            opt.get_help_record(ctx) for opt in self if not opt.hidden  # type: ignore
        ]  # get_help_record() should return None only if opt.hidden

    def option(self, *param_decls: str, **attrs: Any) -> Callable[[F], F]:
        """Refer to :func:`cloup.option`."""
        return option(*param_decls, group=self, **attrs)

    def __iter__(self) -> Iterator[click.Option]:
        return iter(self.options)

    def __getitem__(self, i: int) -> click.Option:
        return self.options[i]

    def __len__(self) -> int:
        return len(self.options)

    def __repr__(self) -> str:
        return make_repr(self, self.title, help=self.help, options=self.options)

    def __str__(self) -> str:
        return make_repr(
            self, self.title, options=[opt.name for opt in self.options])


def has_option_group(param: click.Parameter) -> bool:
    return getattr(param, 'group', None) is not None


def get_option_group_of(param: click.Option) -> Optional[OptionGroup]:
    return getattr(param, 'group', None)


# noinspection PyMethodMayBeStatic
class OptionGroupMixin:
    """Implements support for:

     - option groups
     - the "Positional arguments" help section; this section is shown only if
       at least one of your arguments has non-empty ``help``.

    .. important::
        In order to check the constraints defined on the option groups,
        a command must inherits from :class:`cloup.ConstraintMixin` too!

    .. versionadded:: 0.14.0
        added the "Positional arguments" help section.

    .. versionchanged:: 0.8.0
        this mixin now relies on ``cloup.HelpFormatter`` to align help sections.
        If a ``click.HelpFormatter`` is used with a ``TypeError`` is raised.

    .. versionchanged:: 0.8.0
        removed ``format_option_group``. Added ``get_default_option_group`` and
        ``make_option_group_help_section``.

    .. versionadded:: 0.5.0
    """

    def __init__(
        self, *args: Any, align_option_groups: Optional[bool] = None, **kwargs: Any
    ) -> None:
        """
        :param align_option_groups:
            whether to align the columns of all option groups' help sections.
            This is also available as a context setting having a lower priority
            than this attribute. Given that this setting should be consistent
            across all you commands, you should probably use the context
            setting only.
        :param args:
            positional arguments forwarded to the next class in the MRO
        :param kwargs:
            keyword arguments forwarded to the next class in the MRO
        """
        super().__init__(*args, **kwargs)

        self.align_option_groups = align_option_groups
        params = kwargs.get('params') or []
        arguments, option_groups, ungrouped_options = self._group_params(params)

        self.arguments = arguments

        self.option_groups = option_groups
        """List of all option groups, except the "default option group"."""

        self.ungrouped_options = ungrouped_options
        """List of options not explicitly assigned to an user-defined option group.
        These options will be included in the "default option group".
        **Note:** this list does not include options added automatically by Click
        based on context settings, like the ``--help`` option; use the
        :meth:`get_ungrouped_options` method if you need the real full list
        (which needs a ``Context`` object)."""

    @staticmethod
    def _group_params(
        params: List[Parameter]
    ) -> Tuple[List[click.Argument], List[OptionGroup], List[Option]]:

        options_by_group: Dict[OptionGroup, List[click.Option]] = defaultdict(list)
        arguments: List[click.Argument] = []
        ungrouped_options: List[click.Option] = []
        for param in params:
            if isinstance(param, click.Argument):
                arguments.append(param)
            elif isinstance(param, click.Option):
                grp = get_option_group_of(param)
                if grp is None:
                    ungrouped_options.append(param)
                else:
                    options_by_group[grp].append(param)

        option_groups = list(options_by_group.keys())
        for group, options in options_by_group.items():
            group.options = options

        return arguments, option_groups, ungrouped_options

    def get_ungrouped_options(self, ctx: click.Context) -> Sequence[click.Option]:
        """Return options not explicitly assigned to an option group
        (eventually including the ``--help`` option), i.e. options that will be
        part of the "default option group"."""
        help_option = ctx.command.get_help_option(ctx)
        if help_option is not None:
            return self.ungrouped_options + [help_option]
        else:
            return self.ungrouped_options

    def get_argument_help_record(
        self, arg: click.Argument, ctx: click.Context
    ) -> Tuple[str, str]:
        if isinstance(arg, cloup.Argument):
            return arg.get_help_record(ctx)
        return make_arg_metavar(arg, ctx), ""

    def get_arguments_help_section(self, ctx: click.Context) -> Optional[HelpSection]:
        args_with_help = (arg for arg in self.arguments if getattr(arg, "help", None))
        if not any(args_with_help):
            return None
        return HelpSection(
            heading="Positional arguments",
            definitions=[
                self.get_argument_help_record(arg, ctx) for arg in self.arguments
            ],
        )

    def make_option_group_help_section(
        self, group: OptionGroup, ctx: click.Context
    ) -> HelpSection:
        """Return a ``HelpSection`` for an ``OptionGroup``, i.e. an object containing
        the title, the optional description and the options' definitions for
        this option group.

        .. versionadded:: 0.8.0
        """
        return HelpSection(
            heading=group.title,
            definitions=group.get_help_records(ctx),
            help=group.help,
            constraint=group.constraint.help(ctx) if group.constraint else None
        )

    def must_align_option_groups(
        self, ctx: Optional[click.Context], default: bool = True
    ) -> bool:
        """
        Return ``True`` if the help sections of all options groups should have
        their columns aligned.

        .. versionadded:: 0.8.0
        """
        return first_bool(
            self.align_option_groups,
            getattr(ctx, 'align_option_groups', None),
            default,
        )

    def get_default_option_group(
        self, ctx: click.Context, is_the_only_visible_option_group: bool = False
    ) -> OptionGroup:
        """
        Return an ``OptionGroup`` instance for the options not explicitly
        assigned to an option group, eventually including the ``--help`` option.

        .. versionadded:: 0.8.0
        """
        default_group = OptionGroup(
            "Options" if is_the_only_visible_option_group else "Other options")
        default_group.options = self.get_ungrouped_options(ctx)
        return default_group

    def format_params(
        self, ctx: click.Context, formatter: click.HelpFormatter
    ) -> None:
        formatter = ensure_is_cloup_formatter(formatter)

        visible_sections = []

        # Positional arguments
        positional_arguments_section = self.get_arguments_help_section(ctx)
        if positional_arguments_section:
            visible_sections.append(positional_arguments_section)

        # Option groups
        option_group_sections = [
            self.make_option_group_help_section(group, ctx)
            for group in self.option_groups
            if not group.hidden
        ]
        default_group = self.get_default_option_group(
            ctx, is_the_only_visible_option_group=not option_group_sections
        )
        if not default_group.hidden:
            option_group_sections.append(
                self.make_option_group_help_section(default_group, ctx))

        visible_sections += option_group_sections

        formatter.write_many_sections(
            visible_sections,
            aligned=self.must_align_option_groups(ctx),
        )


@overload
def option_group(
    title: str,
    help: str,
    *options: Decorator,
    constraint: Optional[Constraint] = None,
    hidden: bool = False,
) -> Callable[[F], F]:
    ...


@overload
def option_group(
    title: str,
    *options: Decorator,
    help: Optional[str] = None,
    constraint: Optional[Constraint] = None,
    hidden: bool = False,
) -> Callable[[F], F]:
    ...


# noinspection PyIncorrectDocstring
def option_group(title: str, *args: Any, **kwargs: Any) -> Callable[[F], F]:
    """
    Return a decorator that annotates a function with an option group.

    The ``help`` argument is an optional description and can be provided either
    as keyword argument or as 2nd positional argument after the ``name`` of
    the group::

        # help as keyword argument
        @option_group(name, *options, help=None, ...)

        # help as 2nd positional argument
        @option_group(name, help, *options, ...)

    .. versionchanged:: 0.9.0
        in order to support the decorator :func:`cloup.constrained_params`,
        ``@option_group`` now allows each input decorators to add multiple
        options.

    :param title:
        title of the help section describing the option group.
    :param help:
        an optional description shown below the name; can be provided as keyword
        argument or 2nd positional argument.
    :param options:
        an arbitrary number of decorators like ``click.option``, which attach
        one or multiple options to the decorated command function.
    :param constraint:
        an optional instance of :class:`~cloup.constraints.Constraint`
        (see :doc:`Constraints </pages/constraints>` for more info);
        a description of the constraint will be shown between squared brackets
        aside the option group title (or below it if too long).
    :param hidden:
        if ``True``, the option group and all its options are hidden from the help page
        (all contained options will have their ``hidden`` attribute set to ``True``).
    """
    if args and isinstance(args[0], str):
        return _option_group(title, options=args[1:], help=args[0], **kwargs)
    else:
        return _option_group(title, options=args, **kwargs)


def _option_group(
    title: str,
    options: Sequence[Callable[[F], F]],
    help: Optional[str] = None,
    constraint: Optional[Constraint] = None,
    hidden: bool = False,
) -> Callable[[F], F]:
    if not isinstance(title, str):
        raise TypeError(
            'the first argument of `@option_group` must be its title, a string; '
            'you probably forgot it'
        )

    if not options:
        raise ValueError('you must provide at least one option')

    def decorator(f: F) -> F:
        opt_group = OptionGroup(title, help=help, constraint=constraint, hidden=hidden)
        if not hasattr(f, '__click_params__'):
            f.__click_params__ = []  # type: ignore
        cli_params = f.__click_params__  # type: ignore
        for add_option in reversed(options):
            prev_len = len(cli_params)
            add_option(f)
            added_options = cli_params[prev_len:]
            for new_option in added_options:
                if not isinstance(new_option, Option):
                    raise TypeError(
                        "only parameter of type `Option` can be added to option groups")
                existing_group = get_option_group_of(new_option)
                if existing_group is not None:
                    raise ValueError(
                        f'Option "{new_option}" was first assigned to group '
                        f'"{existing_group}" and then passed as argument to '
                        f'`@option_group({title!r}, ...)`'
                    )
                new_option.group = opt_group  # type: ignore
                if hidden:
                    new_option.hidden = True
        return f

    return decorator
