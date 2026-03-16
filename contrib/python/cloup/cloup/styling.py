"""
This module contains components that specifically address the styling and theming
of the ``--help`` output.
"""
import dataclasses
import dataclasses as dc
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import click

from cloup._util import FrozenSpace, click_version_tuple, delete_keys, identity
from cloup.typing import MISSING, Possibly

IStyle = Callable[[str], str]
"""A callable that takes a string and returns a styled version of it."""


@dataclass(frozen=True)
class HelpTheme:
    """A collection of styles for several elements of the help page.

    A "style" is just a function or a callable that takes a string and returns
    a styled version of it. This means you can use your favorite styling/color
    library (like rich, colorful etc). Nonetheless, given that Click has some
    basic styling functionality built-in, Cloup provides the :class:`Style`
    class, which is a wrapper of the ``click.style`` function.

    :param invoked_command:
        Style of the invoked command name (in Usage).
    :param command_help:
        Style of the invoked command description (below Usage).
    :param heading:
        Style of help section headings.
    :param constraint:
        Style of an option group constraint description.
    :param section_help:
        Style of the help text of a section (the optional paragraph below the heading).
    :param col1:
        Style of the first column of a definition list (options and command names).
    :param col2:
        Style of the second column of a definition list (help text).
    :param epilog:
        Style of the epilog.
    :param alias:
        Style of subcommand aliases in a definition lists.
    :param alias_secondary:
        Style of separator and eventual parenthesis/brackets in subcommand alias lists.
        If not provided, the ``alias`` style will be used.
    """

    invoked_command: IStyle = identity
    """Style of the invoked command name (in Usage)."""

    command_help: IStyle = identity
    """Style of the invoked command description (below Usage)."""

    heading: IStyle = identity
    """Style of help section headings."""

    constraint: IStyle = identity
    """Style of an option group constraint description."""

    section_help: IStyle = identity
    """Style of the help text of a section (the optional paragraph below the heading)."""

    col1: IStyle = identity
    """Style of the first column of a definition list (options and command names)."""

    col2: IStyle = identity
    """Style of the second column of a definition list (help text)."""

    alias: IStyle = identity
    """Style of subcommand aliases in a definition lists."""

    alias_secondary: Optional[IStyle] = None
    """Style of separator and eventual parenthesis/brackets in subcommand alias lists.
    If not provided, the ``alias`` style will be used."""

    epilog: IStyle = identity
    """Style of the epilog."""

    def with_(
        self, invoked_command: Optional[IStyle] = None,
        command_help: Optional[IStyle] = None,
        heading: Optional[IStyle] = None,
        constraint: Optional[IStyle] = None,
        section_help: Optional[IStyle] = None,
        col1: Optional[IStyle] = None,
        col2: Optional[IStyle] = None,
        alias: Optional[IStyle] = None,
        alias_secondary: Possibly[Optional[IStyle]] = MISSING,
        epilog: Optional[IStyle] = None,
    ) -> 'HelpTheme':
        kwargs = {key: val for key, val in locals().items() if val is not None}
        if alias_secondary is MISSING:
            del kwargs["alias_secondary"]
        kwargs.pop('self')
        if kwargs:
            return dataclasses.replace(self, **kwargs)
        return self

    @staticmethod
    def dark() -> "HelpTheme":
        """A theme assuming a dark terminal background color."""
        return HelpTheme(
            invoked_command=Style(fg='bright_yellow'),
            heading=Style(fg='bright_white', bold=True),
            constraint=Style(fg='magenta'),
            col1=Style(fg='bright_yellow'),
            alias=Style(fg='yellow'),
            alias_secondary=Style(fg='white'),
        )

    @staticmethod
    def light() -> "HelpTheme":
        """A theme assuming a light terminal background color."""
        return HelpTheme(
            invoked_command=Style(fg='yellow'),
            heading=Style(fg='bright_blue'),
            constraint=Style(fg='red'),
            col1=Style(fg='yellow'),
        )


@dc.dataclass(frozen=True)
class Style:
    """Wraps :func:`click.style` for a better integration with :class:`HelpTheme`.

    Available colors are defined as static constants in :class:`Color`.

    Arguments are set to ``None`` by default. Passing ``False`` to boolean args
    or ``Color.reset`` as color causes a reset code to be inserted.

    With respect to :func:`click.style`, this class:

    - has an argument less, ``reset``, which is always ``True``
    - add the ``text_transform``.

    .. warning::
        The arguments ``overline``, ``italic`` and ``strikethrough`` are only
        supported in Click 8 and will be ignored if you are using Click 7.

    :param fg: foreground color
    :param bg: background color
    :param bold:
    :param dim:
    :param underline:
    :param overline:
    :param italic:
    :param blink:
    :param reverse:
    :param strikethrough:
    :param text_transform:
        a generic string transformation; useful to apply functions like ``str.upper``

    .. versionadded:: 0.8.0
    """
    fg: Optional[str] = None
    bg: Optional[str] = None
    bold: Optional[bool] = None
    dim: Optional[bool] = None
    underline: Optional[bool] = None
    overline: Optional[bool] = None
    italic: Optional[bool] = None
    blink: Optional[bool] = None
    reverse: Optional[bool] = None
    strikethrough: Optional[bool] = None
    text_transform: Optional[IStyle] = None

    _style_kwargs: Optional[Dict[str, Any]] = dc.field(init=False, default=None)

    def __call__(self, text: str) -> str:
        if self._style_kwargs is None:
            kwargs = dc.asdict(self)
            delete_keys(kwargs, ['text_transform', '_style_kwargs'])
            if int(click_version_tuple[0]) < 8:
                # These arguments are not supported in Click < 8. Ignore them.
                delete_keys(kwargs, ['overline', 'italic', 'strikethrough'])
            object.__setattr__(self, '_style_kwargs', kwargs)
        else:
            kwargs = self._style_kwargs

        if self.text_transform:
            text = self.text_transform(text)
        return click.style(text, **kwargs)


class Color(FrozenSpace):
    """Colors accepted by :class:`Style` and :func:`click.style`."""
    black = "black"
    red = "red"
    green = "green"
    yellow = "yellow"
    blue = "blue"
    magenta = "magenta"
    cyan = "cyan"
    white = "white"
    reset = "reset"
    bright_black = "bright_black"
    bright_red = "bright_red"
    bright_green = "bright_green"
    bright_yellow = "bright_yellow"
    bright_blue = "bright_blue"
    bright_magenta = "bright_magenta"
    bright_cyan = "bright_cyan"
    bright_white = "bright_white"


DEFAULT_THEME = HelpTheme()
