from collections import OrderedDict
from typing import (
    Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type, TypeVar, Union,
)

import click

from cloup._util import first_bool, pick_not_none
from cloup.formatting import HelpSection, ensure_is_cloup_formatter

CommandType = TypeVar('CommandType', bound=Type[click.Command])
Subcommands = Union[Iterable[click.Command], Dict[str, click.Command]]


class Section:
    """
    A group of (sub)commands to show in the same help section of a
    ``MultiCommand``. You can use sections with any `Command` that inherits
    from :class:`SectionMixin`.

    .. versionchanged:: 0.6.0
        removed the deprecated old name ``GroupSection``.

    .. versionchanged:: 0.5.0
        introduced the new name ``Section`` and deprecated the old ``GroupSection``.
    """

    def __init__(self, title: str,
                 commands: Subcommands = (),
                 is_sorted: bool = False):  # noqa
        """
        :param title:
        :param commands: sequence of commands or dict of commands keyed by name
        :param is_sorted:
            if True, ``list_commands()`` returns the commands in lexicographic order
        """
        if not isinstance(title, str):
            raise TypeError(
                'the first argument must be a string, the title; you probably forgot it')
        self.title = title
        self.is_sorted = is_sorted
        self.commands: OrderedDict[str, click.Command] = OrderedDict()
        if isinstance(commands, Sequence):
            self.commands = OrderedDict()
            for cmd in commands:
                self.add_command(cmd)
        elif isinstance(commands, dict):
            self.commands = OrderedDict(commands)
        else:
            raise TypeError('argument `commands` must be a sequence of commands '
                            'or a dict of commands keyed by name')

    @classmethod
    def sorted(cls, title: str, commands: Subcommands = ()) -> 'Section':
        return cls(title, commands, is_sorted=True)

    def add_command(self, cmd: click.Command, name: Optional[str] = None) -> None:
        name = name or cmd.name
        if not name:
            raise TypeError('missing command name')
        if name in self.commands:
            raise Exception(f'command "{name}" already exists')
        self.commands[name] = cmd

    def list_commands(self) -> List[Tuple[str, click.Command]]:
        command_list = [(name, cmd) for name, cmd in self.commands.items()
                        if not cmd.hidden]
        if self.is_sorted:
            command_list.sort()
        return command_list

    def __len__(self) -> int:
        return len(self.commands)

    def __repr__(self) -> str:
        return 'Section({}, is_sorted={})'.format(self.title, self.is_sorted)


class SectionMixin:
    """
    Adds to a :class:`click.MultiCommand` the possibility of organizing its subcommands
    into multiple help sections.

    Sections can be specified in the following ways:

    #. passing a list of :class:`Section` objects to the constructor setting
       the argument ``sections``
    #. using :meth:`add_section` to add a single section
    #. using :meth:`add_command` with the argument `section` set

    Commands not assigned to any user-defined section are added to the
    "default section", whose title is "Commands" or "Other commands" depending
    on whether it is the only section or not. The default section is the last
    shown section in the help and its commands are listed in lexicographic order.

    .. versionchanged:: 0.8.0
        this mixin now relies on ``cloup.HelpFormatter`` to align help sections.
        If a ``click.HelpFormatter`` is used with a ``TypeError`` is raised.

    .. versionchanged:: 0.8.0
        removed ``format_section``. Added ``make_commands_help_section``.

    .. versionadded:: 0.5.0
    """

    def __init__(
        self, *args: Any,
        commands: Optional[Dict[str, click.Command]] = None,
        sections: Iterable[Section] = (),
        align_sections: Optional[bool] = None,
        **kwargs: Any,
    ):
        """
        :param align_sections:
            whether to align the columns of all subcommands' help sections.
            This is also available as a context setting having a lower priority
            than this attribute. Given that this setting should be consistent
            across all you commands, you should probably use the context
            setting only.
        :param args:
            positional arguments forwarded to the next class in the MRO
        :param kwargs:
            keyword arguments forwarded to the next class in the MRO
        """
        super().__init__(*args, commands=commands, **kwargs)  # type: ignore
        self.align_sections = align_sections
        self._default_section = Section('__DEFAULT', commands=commands or [])
        self._user_sections: List[Section] = []
        self._section_set = {self._default_section}
        for section in sections:
            self.add_section(section)

    def _add_command_to_section(
        self, cmd: click.Command,
        name: Optional[str] = None,
        section: Optional[Section] = None
    ) -> None:
        """Add a command to the section (if specified) or to the default section."""
        name = name or cmd.name
        if section is None:
            section = self._default_section
        section.add_command(cmd, name)
        if section not in self._section_set:
            self._user_sections.append(section)
            self._section_set.add(section)

    def add_section(self, section: Section) -> None:
        """Add a :class:`Section` to this group. You can add the same
        section object only a single time.

        See Also:
            :meth:`section`
        """
        if section in self._section_set:
            raise ValueError(f'section "{section}" was already added')
        self._user_sections.append(section)
        self._section_set.add(section)
        for name, cmd in section.commands.items():
            # It's important to call self.add_command() and not super().add_command() here
            # otherwise subclasses' add_command() is not called.
            self.add_command(cmd, name, fallback_to_default_section=False)

    def section(self, title: str, *commands: click.Command, **attrs: Any) -> Section:
        """Create a new :class:`Section`, adds it to this group and returns it."""
        section = Section(title, commands, **attrs)
        self.add_section(section)
        return section

    def add_command(
        self, cmd: click.Command,
        name: Optional[str] = None,
        section: Optional[Section] = None,
        fallback_to_default_section: bool = True,
    ) -> None:
        """
        Add a subcommand to this ``Group``.

        **Implementation note:** ``fallback_to_default_section`` looks not very
        clean but, even if it's not immediate to see (it wasn't for me), I chose
        it over apparently cleaner options.

        :param cmd:
        :param name:
        :param section:
            a ``Section`` instance. The command must not be in the section already.
        :param fallback_to_default_section:
            if ``section`` is None and this option is enabled, the command is added
            to the "default section". If disabled, the command is not added to
            any section unless ``section`` is provided. This is useful for
            internal code and subclasses. Don't disable it unless you know what
            you are doing.
        """
        super().add_command(cmd, name)  # type: ignore
        if section or fallback_to_default_section:
            self._add_command_to_section(cmd, name, section)

    def list_sections(
        self, ctx: click.Context, include_default_section: bool = True
    ) -> List[Section]:
        """
        Return the list of all sections in the "correct order".

        If ``include_default_section=True`` and the default section is non-empty,
        it will be included at the end of the list.
        """
        section_list = list(self._user_sections)
        if include_default_section and len(self._default_section) > 0:
            default_section = Section.sorted(
                title='Other commands' if len(self._user_sections) > 0 else 'Commands',
                commands=self._default_section.commands)
            section_list.append(default_section)
        return section_list

    def format_subcommand_name(
        self, ctx: click.Context, name: str, cmd: click.Command
    ) -> str:
        """Used to format the name of the subcommands. This method is useful
        when you combine this extension with other click extensions that override
        :meth:`format_commands`. Most of these, like click-default-group, just
        add something to the name of the subcommands, which is exactly what this
        method allows you to do without overriding bigger methods.
        """
        return name

    def make_commands_help_section(
        self, ctx: click.Context, section: Section
    ) -> Optional[HelpSection]:
        visible_subcommands = section.list_commands()
        if not visible_subcommands:
            return None
        return HelpSection(
            heading=section.title,
            definitions=[
                (self.format_subcommand_name(ctx, name, cmd), cmd.get_short_help_str)
                for name, cmd in visible_subcommands
            ]
        )

    def must_align_sections(
        self, ctx: Optional[click.Context], default: bool = True
    ) -> bool:
        return first_bool(
            self.align_sections,
            getattr(ctx, 'align_sections', None),
            default,
        )

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        formatter = ensure_is_cloup_formatter(formatter)

        subcommand_sections = self.list_sections(ctx)
        help_sections = pick_not_none(
            self.make_commands_help_section(ctx, section)
            for section in subcommand_sections
        )
        if not help_sections:
            return

        formatter.write_many_sections(
            help_sections, aligned=self.must_align_sections(ctx)
        )
