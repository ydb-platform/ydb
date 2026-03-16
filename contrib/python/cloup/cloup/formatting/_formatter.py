import dataclasses as dc
import inspect
import shutil
import textwrap
from itertools import chain
from typing import (
    Any, Callable, Dict, Iterable, Iterator, Optional, Sequence, TYPE_CHECKING,
    Tuple, Union,
)

from cloup._util import click_version_ge_8_1
from cloup.formatting._util import unstyled_len

if TYPE_CHECKING:
    from .sep import RowSepPolicy, SepGenerator

import click
from click.formatting import wrap_text

from cloup._util import (
    check_positive_int, identity, indent_lines, make_repr,
    pick_non_missing,
)
from ..typing import MISSING, Possibly
from cloup.styling import HelpTheme, IStyle

Definition = Tuple[str, Union[str, Callable[[int], str]]]


@dc.dataclass()
class HelpSection:
    """A container for a help section data."""
    heading: str
    """Help section title."""

    definitions: Sequence[Definition]
    """Rows with 2 columns each. The 2nd element of each row can also be a function
    taking an integer (the available width for the 2nd column) and returning a string."""

    help: Optional[str] = None
    """(Optional) long description of the section."""

    constraint: Optional[str] = None
    """(Optional) option group constraint description."""


# noinspection PyMethodMayBeStatic
class HelpFormatter(click.HelpFormatter):
    """
    A custom help formatter. Features include:

    - more attributes for controlling the output of the formatter
    - a ``col1_width`` parameter in :meth:`write_dl` that allows Cloup to align
      multiple definition lists without resorting to hacks
    - a "linear layout" for definition lists that kicks in when the available
      terminal width is not enough for the standard 2-column layout
      (see argument ``col2_min_width``)
    - the first column width, when not explicitly given in ``write_dl`` is
      computed excluding the rows that exceed ``col1_max_width``
      (called ``col_max`` in ``write_dl`` for compatibility with Click).

    .. versionchanged:: 0.9.0
        the ``row_sep`` parameter now:

        - is set to ``None`` by default and ``row_sep=""`` corresponds to an
          empty line between rows
        - must not ends with ``\\n``; the formatter writes a newline just after
          it (when it's not ``None``), so a newline at the end is always enforced
        - accepts instances of :class:`~cloup.formatting.sep.SepGenerator` and
          :class:`~cloup.formatting.sep.RowSepPolicy`.

    .. versionadded:: 0.8.0

    :param indent_increment:
        width of each indentation increment.
    :param width:
        content line width, excluding the newline character; by default it's
        initialized to ``min(terminal_width - 1, max_width)`` where
        ``max_width`` is another argument.
    :param max_width:
        maximum content line width (equivalent to ``Context.max_content_width``).
        Used to compute ``width`` when it is not provided, ignored otherwise.
    :param col1_max_width:
        the maximum width of the first column of a definition list; as in Click,
        if the text of a row exceeds this threshold, the 2nd column is printed
        on a new line.
    :param col2_min_width:
        the minimum width for the second column of a definition list; if the
        available space is less than this value, the formatter switches from the
        standard 2-column layout to the "linear layout" (that this decision
        is taken for each definition list). If you want to always use the linear
        layout, you can set this argument to a very high number (or ``math.inf``).
        If you never want it (not recommended), you can set this argument to zero.
    :param col_spacing:
        the number of spaces between the column boundaries of a definition list.
    :param row_sep:
        an "extra" separator to insert between the rows of a definition list (in
        addition to the normal newline between definitions). If you want an empty
        line between rows, pass ``row_sep=""``.
        Read :ref:`Row separators <row-separators>` for more.
    :param theme:
        an :class:`~cloup.HelpTheme` instance specifying how to style the various
        elements of the help page.
    """

    def __init__(
        self, indent_increment: int = 2,
        width: Optional[int] = None,
        max_width: Optional[int] = None,
        col1_max_width: int = 30,
        col2_min_width: int = 35,
        col_spacing: int = 2,
        row_sep: Union[None, str, 'SepGenerator', 'RowSepPolicy'] = None,
        theme: HelpTheme = HelpTheme(),
    ):
        check_positive_int(col1_max_width, 'col1_max_width')
        check_positive_int(col_spacing, 'col_spacing')
        if isinstance(row_sep, str) and row_sep.endswith('\n'):
            raise ValueError(
                "since v0.9, row_sep must not end with '\\n'. The formatter writes "
                "a '\\n' after it; no other newline is allowed.\n"
                "If you want an empty line between rows, set row_sep=''.")

        max_width = max_width or 80
        # We subtract 1 to the terminal width to leave space for the new line character.
        # Otherwise, when we write a line that is long exactly terminal_size (without \n)
        # the \n is printed on a new terminal line, leading to a useless empty line.
        width = (
            width
            or click.formatting.FORCED_WIDTH
            or min(max_width, shutil.get_terminal_size((80, 100)).columns - 1)
        )
        super().__init__(
            width=width, max_width=max_width, indent_increment=indent_increment
        )
        self.width: int = width
        self.col1_max_width = col1_max_width
        self.col2_min_width = col2_min_width
        self.col_spacing = col_spacing
        self.theme = theme
        self.row_sep = row_sep

    @staticmethod
    def settings(
        *, width: Possibly[Optional[int]] = MISSING,
        max_width: Possibly[Optional[int]] = MISSING,
        indent_increment: Possibly[int] = MISSING,
        col1_max_width: Possibly[int] = MISSING,
        col2_min_width: Possibly[int] = MISSING,
        col_spacing: Possibly[int] = MISSING,
        row_sep: Possibly[Union[None, str, 'SepGenerator', 'RowSepPolicy']] = MISSING,
        theme: Possibly[HelpTheme] = MISSING,
    ) -> Dict[str, Any]:
        """A utility method for creating a ``formatter_settings`` dictionary to
        pass as context settings or command attribute. This method exists for
        one only reason: it enables auto-complete for formatter options, thus
        improving the developer experience.

        Parameters are described in :class:`HelpFormatter`.
        """
        return pick_non_missing(locals())

    @property
    def available_width(self) -> int:
        return self.width - self.current_indent

    def write(self, *strings: str) -> None:
        self.buffer += strings

    def write_usage(
        self, prog: str, args: str = "", prefix: Optional[str] = None
    ) -> None:
        prefix = "Usage:" if prefix is None else prefix
        prefix = self.theme.heading(prefix) + " "
        prog = self.theme.invoked_command(prog)
        super().write_usage(prog, args, prefix)

    def write_aliases(self, aliases: Sequence[str]) -> None:
        self.write_heading("Aliases", newline=False)
        alias_list = ", ".join(self.theme.col1(alias) for alias in aliases)
        self.write(f" {alias_list}\n")

    def write_command_help_text(self, cmd: click.Command) -> None:
        help_text = cmd.help or ""
        if help_text and click_version_ge_8_1:
            help_text = inspect.cleandoc(help_text).partition("\f")[0]
        if cmd.deprecated:
            # Use the same label as Click:
            # https://github.com/pallets/click/blob/b0538df/src/click/core.py#L1331
            help_text = "(Deprecated) " + help_text
        if help_text:
            self.write_paragraph()
            with self.indentation():
                self.write_text(help_text, style=self.theme.command_help)

    def write_heading(self, heading: str, newline: bool = True) -> None:
        if self.current_indent:
            self.write(" " * self.current_indent)
        self.write(self.theme.heading(heading + ":"))
        if newline:
            self.write('\n')

    def write_many_sections(
        self, sections: Sequence[HelpSection],
        aligned: bool = True,
    ) -> None:
        if aligned:
            return self.write_aligned_sections(sections)
        for s in sections:
            self.write_section(s)

    def write_aligned_sections(self, sections: Sequence[HelpSection]) -> None:
        """Write multiple aligned definition lists."""
        all_rows = chain.from_iterable(dl.definitions for dl in sections)
        col1_width = self.compute_col1_width(all_rows, self.col1_max_width)
        for s in sections:
            self.write_section(s, col1_width=col1_width)

    def write_section(self, s: HelpSection, col1_width: Optional[int] = None) -> None:
        theme = self.theme
        self.write("\n")
        self.write_heading(s.heading, newline=not s.constraint)
        if s.constraint:
            constraint_text = f'[{s.constraint}]'
            available_width = self.available_width - len(s.heading) - len(': ')
            if len(constraint_text) <= available_width:
                self.write(" ", theme.constraint(constraint_text), "\n")
            else:
                self.write("\n")
                with self.indentation():
                    self.write_text(constraint_text, theme.constraint)

        with self.indentation():
            if s.help:
                self.write_text(s.help, theme.section_help)
            self.write_dl(s.definitions, col1_width=col1_width)

    def write_text(self, text: str, style: IStyle = identity) -> None:
        wrapped = wrap_text(
            text, self.width - self.current_indent, preserve_paragraphs=True)
        if style is identity:
            wrapped_text = textwrap.indent(wrapped, prefix=' ' * self.current_indent)
        else:
            styled_lines = map(style, wrapped.splitlines())
            lines = indent_lines(styled_lines, width=self.current_indent)
            wrapped_text = "\n".join(lines)
        self.write(wrapped_text, "\n")

    def compute_col1_width(self, rows: Iterable[Definition], max_width: int) -> int:
        col1_lengths = (unstyled_len(r[0]) for r in rows)
        lengths_under_limit = (length for length in col1_lengths if length <= max_width)
        return max(lengths_under_limit, default=0)

    def write_dl(
        self, rows: Sequence[Definition],
        col_max: Optional[int] = None,  # default changed to None wrt parent class
        col_spacing: Optional[int] = None,  # default changed to None wrt parent class
        col1_width: Optional[int] = None,
    ) -> None:
        """Write a definition list into the buffer. This is how options
        and commands are usually formatted.

        If there's enough space, definition lists are rendered as a 2-column
        pseudo-table: if the first column text of a row doesn't fit in the
        provided/computed ``col1_width``, the 2nd column is printed on the
        following line.

        If the available space for the 2nd column is below ``self.col2_min_width``,
        the 2nd "column" is always printed below the 1st, indented with a minimum
        of 3 spaces (or one ``indent_increment`` if that's greater than 3).

        :param rows:
            a list of two item tuples for the terms and values.
        :param col_max:
            the maximum width for the 1st column of a definition list; this
            argument is here to not break compatibility with Click; if provided,
            it overrides the attribute ``self.col1_max_width``.
        :param col_spacing:
            number of spaces between the first and second column;
            this argument is here to not break compatibility with Click;
            if provided, it overrides ``self.col_spacing``.
        :param col1_width:
            the width to use for the first column; if not provided, it's
            computed as the length of the longest string under ``self.col1_max_width``;
            useful when you need to align multiple definition lists.
        """
        # |<----------------------- width ------------------------>|
        # |                |<---------- available_width ---------->|
        # | current_indent | col1_width | col_spacing | col2_width |

        col1_max_width = min(
            col_max or self.col1_max_width,
            self.available_width,
        )
        col1_width = min(
            col1_width or self.compute_col1_width(rows, col1_max_width),
            col1_max_width,
        )
        col_spacing = col_spacing or self.col_spacing
        col2_width = self.available_width - col1_width - col_spacing

        if col2_width < self.col2_min_width:
            self.write_linear_dl(rows)
        else:
            self.write_tabular_dl(rows, col1_width, col_spacing, col2_width)

    def _get_row_sep_for(
        self, text_rows: Sequence[Sequence[str]],
        col_widths: Sequence[int],
        col_spacing: int,
    ) -> Optional[str]:
        if self.row_sep is None or isinstance(self.row_sep, str):
            return self.row_sep

        from .sep import RowSepPolicy
        if isinstance(self.row_sep, RowSepPolicy):
            return self.row_sep(text_rows, col_widths, col_spacing)
        elif callable(self.row_sep):  # RowSepPolicy is callable; keep this for last
            return self.row_sep(self.available_width)
        else:
            raise TypeError('row_sep')

    def write_tabular_dl(
        self, rows: Sequence[Definition],
        col1_width: int, col_spacing: int, col2_width: int,
    ) -> None:
        """Format a definition list as a 2-column "pseudo-table". If the first
        column of a row exceeds ``col1_width``, the 2nd column is written on
        the subsequent line. This is the standard way of formatting definition
        lists and it's the default if there's enough space."""

        col1_plus_spacing = col1_width + col_spacing
        col2_indentation = " " * (
            self.current_indent + max(self.indent_increment, col1_plus_spacing)
        )
        indentation = " " * self.current_indent

        # Note: iter_defs() resolves eventual callables in row[1]
        text_rows = list(iter_defs(rows, col2_width))
        row_sep = self._get_row_sep_for(text_rows, (col1_width, col2_width), col_spacing)
        col1_styler, col2_styler = self.theme.col1, self.theme.col2

        def write_row(row: Tuple[str, str]) -> None:
            first, second = row
            self.write(indentation, col1_styler(first))
            if not second:
                self.write("\n")
            else:
                first_display_length = unstyled_len(first)
                if first_display_length <= col1_width:
                    spaces_to_col2 = col1_plus_spacing - first_display_length
                    self.write(" " * spaces_to_col2)
                else:
                    self.write("\n", col2_indentation)

                if len(second) <= col2_width:
                    self.write(col2_styler(second), "\n")
                else:
                    wrapped_text = wrap_text(second, col2_width, preserve_paragraphs=True)
                    lines = [col2_styler(line) for line in wrapped_text.splitlines()]
                    self.write(lines[0], "\n")
                    for line in lines[1:]:
                        self.write(col2_indentation, line, "\n")

        write_row(text_rows[0])
        for row in text_rows[1:]:
            if row_sep is not None:
                self.write(indentation, row_sep, "\n")
            write_row(row)

    def write_linear_dl(self, dl: Sequence[Definition]) -> None:
        """Format a definition list as a "linear list". This is the default when
        the available width for the definitions (2nd column) is below
        ``self.col2_min_width``."""
        help_extra_indent = max(3, self.indent_increment)
        help_total_indent = self.current_indent + help_extra_indent
        help_max_width = self.width - help_total_indent
        current_indentation = " " * self.current_indent

        col1_styler = self.theme.col1
        col2_styler = self.theme.col2

        for names, help in iter_defs(dl, help_max_width):
            self.write(current_indentation + col1_styler(names) + '\n')
            if help:
                self.current_indent += help_extra_indent
                self.write_text(help, col2_styler)
                self.current_indent -= help_extra_indent
            self.write("\n")
        self.buffer.pop()  # pop last newline

    def write_epilog(self, epilog: str) -> None:
        self.write_text(epilog, self.theme.epilog)

    def __repr__(self) -> str:
        return make_repr(
            self, width=self.width, indent_increment=self.indent_increment,
            col1_max_width=self.col1_max_width, col_spacing=self.col_spacing
        )


def iter_defs(rows: Iterable[Definition], col2_width: int) -> Iterator[Tuple[str, str]]:
    for row in rows:
        if len(row) == 1:
            yield row[0], ''
        elif len(row) == 2:
            second = row[1](col2_width) if callable(row[1]) else row[1]
            yield row[0], second
        else:
            raise ValueError(f'invalid row length: {len(row)}')
