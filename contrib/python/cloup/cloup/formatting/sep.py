"""
This module contains anything related to separators. Currently, it contains an
implementation of "row sep policies", e.g. components that decide if and how to
add an extra separator/spacing between the rows of a definition list (only for
tabular layout). In the future, it may be expanded with something analogous for
help sections.
"""
import abc
from itertools import zip_longest
from typing import Optional, Protocol, Sequence, Union

SepType = Union[str, 'SepGenerator']


class SepGenerator(Protocol):
    """Generate a separator given a width. When used as ``row_sep``, this ``width``
    corresponds to ``HelpFormatter.available_width``, i.e. the line width excluding
    the current indentation width.

    Note: the length of the returned separator may differ from ``width``.
    """

    def __call__(self, width: int) -> str:
        ...


class RowSepPolicy(metaclass=abc.ABCMeta):
    """A callable that can be passed as ``row_sep`` to  :class:`HelpFormatter` in
    order to decide *if* a definition list should get a row separator (in
    addition to ``\\n``) and *which* separator.

    In practice, the row separator should be the same for all definition lists
    that satisfy a given condition. That's why :class:`RowSepIf`
    exists, you probably want to use that.

    Nonetheless, this protocol exists mainly for one reason: it leaves open the
    door to policies that can decide a row separator for each individual row
    (feature I'm personally against to for now), without breaking changes.
    This would make possible to implement the old Click 7.2 behavior, which
    inserted an empty line only after options with a long help. Adding this
    feature would be possible without breaking changes, by extending the return
    type to ``Union[None, str, Sequence[str]]``.
    """

    @abc.abstractmethod
    def __call__(  # noqa E704
        self, rows: Sequence[Sequence[str]],
        col_widths: Sequence[int],
        col_spacing: int,
    ) -> Optional[str]:
        """Decide which row separator to use (eventually none) in the given
        definition list."""


class RowSepCondition(Protocol):
    """Determines when a definition list should use a row separator."""

    # Ignore error due to flake8 issue: "multiple statements on one line (def)"
    def __call__(  # noqa E704
        self, rows: Sequence[Sequence[str]],
        col_widths: Sequence[int],
        col_spacing: int,
    ) -> bool:
        """Return ``True`` if the input definition list should use a row
        separator (in addition to the usual ``\\n``)."""


class RowSepIf(RowSepPolicy):
    """
    Inserts a row separator between the rows of a definition list only if a
    condition is satisfied. This class implements the ``RowSepPolicy``
    protocol and does two things:

    - enforces the use of a single row separator for all rows of a
        definition lists and for all definition lists; note that
        ``RowSepPolicy`` doesn't for implementation reasons but it's probably
        what you want;
    - allows you to implement different conditions (see type
        :data:`RowSepCondition`) without worrying about the generation part,
        which is always the same.

    :param condition:
        a :class:`RowSepCondition` that determines when to add the (extra)
        row separator.
    :param sep:
        either a string or a ``SepGenerator``,
        i.e. a function ``(width: int) -> str`` (e.g. :class:`Hline`).
        The empty string corresponds to an empty line separator.
    """

    def __init__(self, condition: RowSepCondition,
                 sep: Union[str, SepGenerator] = ''):
        if isinstance(sep, str) and sep.endswith('\n'):
            raise ValueError(
                "sep must not end with '\\n'. The formatter writes  a '\\n' after it; "
                "no other newline is allowed.")
        self.condition = condition
        self.sep = sep

    def __call__(
        self, rows: Sequence[Sequence[str]], col_widths: Sequence[int], col_spacing: int
    ) -> Optional[str]:
        if self.condition(rows, col_widths, col_spacing):
            if callable(self.sep):
                total_width = get_total_width(col_widths, col_spacing)
                return self.sep(total_width)
            return self.sep
        return None


# ==========================================
#  Conditions & related utils

def get_total_width(col_widths: Sequence[int], col_spacing: int) -> int:
    """Return the total width of a definition list (or, more generally, a table).
    Useful when implementing a RowSepStrategy."""
    return sum(col_widths) + col_spacing * (len(col_widths) - 1)


def count_multiline_rows(rows: Sequence[Sequence[str]], col_widths: Sequence[int]) -> int:
    # Note: I'm using zip_longest on purpose so that a TypeError will be raised
    # if len(row) != len(col_widths). An explicit check is not worth it since
    # this should never happen.
    return sum(
        any(len(col_text) > col_width
            for col_text, col_width in zip_longest(row, col_widths))
        for row in rows
    )


def multiline_rows_are_at_least(
    count_or_percentage: Union[int, float]
) -> RowSepCondition:
    """
    Return a ``RowSepStrategy`` that returns a row separator between all rows
    of a definition list, only if the number of rows taking multiple lines is
    greater than or equal to a certain threshold.

    :param count_or_percentage:
        a threshold for multiline rows above which the returned strategy will
        insert a row separator. It can be either an absolute count (`int`) or a
        percentage relative to the total number of rows expressed as a `float`
        between 0 and 1 (0 and 1 excluded).
    """
    if count_or_percentage <= 0:
        raise ValueError('count_or_percentage should be > 0')

    if isinstance(count_or_percentage, int):
        count_threshold = count_or_percentage

        def condition(
            rows: Sequence[Sequence[str]],
            col_widths: Sequence[int],
            col_spacing: int,
        ) -> bool:
            num_multiline = count_multiline_rows(rows, col_widths)
            return num_multiline >= count_threshold

    elif isinstance(count_or_percentage, float):
        percent_threshold = count_or_percentage
        if percent_threshold > 1.0:
            raise ValueError(
                "count_or_percentage must be either an integer or a float in the "
                f"interval ]0, 1[. You passed a float >= 1.0 ({percent_threshold}).")

        def condition(
            rows: Sequence[Sequence[str]],
            col_widths: Sequence[int],
            col_spacing: int,
        ) -> bool:
            num_multiline = count_multiline_rows(rows, col_widths)
            percent_multiline = num_multiline / len(rows)
            return percent_multiline >= percent_threshold
    else:
        raise TypeError('count_or_percentage must be an int or a float')

    return condition


class Hline(SepGenerator):
    """Returns a function that generates an horizontal line of a given length.

    This class has different static members for different line styles
    like ``Hline.solid``, ``Hline.dashed``, ``Hline.densely_dashed``
    and  ``Hline.dotted``.

    :param pattern:
        a string (usually a single character) that is repeated to generate
        the line.
    """

    # Workaround: PyCharm auto-completion doesn't work without these declarations
    solid: 'Hline'
    dashed: 'Hline'
    densely_dashed: 'Hline'
    dotted: 'Hline'

    def __init__(self, pattern: str):
        self.pattern = pattern

    def __call__(self, width: int) -> str:
        pattern = self.pattern
        if len(pattern) == 1:
            return pattern * width
        reps, rest = width // len(pattern), width % len(pattern)
        return pattern * reps + pattern[:rest]


Hline.solid = Hline("─")
"""Return a line like ``────────``."""

Hline.dashed = Hline('-')
"""Return a line like ``--------``."""

Hline.densely_dashed = Hline('╌')
"""Return a line like ``╌╌╌╌╌╌╌╌``."""

Hline.dotted = Hline("┄")
"""Return a line like ``┄┄┄┄┄┄┄┄``."""
