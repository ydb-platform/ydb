import dataclasses
import itertools
import sys

from typing import Optional, TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from typing import Self
    from debian._deb822_repro.parsing import Deb822Element


_DATA_CLASS_OPTIONAL_ARGS = {}
if sys.version_info >= (3, 10):
    # The `slots` feature greatly reduces the memory usage by avoiding the `__dict__`
    # instance. But at the end of the day, performance is "nice to have" for this
    # feature and all current consumers are at Python 3.12 (except the CI tests...)
    _DATA_CLASS_OPTIONAL_ARGS["slots"] = True


@dataclasses.dataclass(frozen=True, **_DATA_CLASS_OPTIONAL_ARGS)
class Position:
    """Describes a "cursor" position inside a file

    It consists of a line position (0-based line number) and a cursor position.  This is modelled
    after the "Position" in Language Server Protocol (LSP).
    """

    line_position: int
    """Describes the line position as a 0-based line number

    See line_number if you want a human-readable line number
    """
    cursor_position: int
    """Describes a cursor position ("between two characters") or a character offset.

    When this value is 0, the position is at the start of a line. When it is 1, then
    the position is between the first and the second character (etc.).
    """

    @property
    def line_number(self) -> int:
        """The line number as human would count it"""
        return self.line_position + 1

    def relative_to(self, new_base: "Position") -> "Position":
        """Offsets the position relative to another position

        This is useful to avoid the `position_in_file()` method by caching where
        the parents position and then for its children you use `range_in_parent()`
        plus `relative_to()` to rebase the range.

        >>> parent: Locatable = ...                   # doctest: +SKIP
        >>> children: Iterable[Locatable] = ...       # doctest: +SKIP
        >>> # This will expensive
        >>> parent_pos = parent.position_in_file()     # doctest: +SKIP
        >>> for child in children:                    # doctest: +SKIP
        ...    child_pos = child.position_in_parent()
        ...    # Avoid a position_in_file() for each child
        ...    child_pos_in_file = child_pos.relative_to(parent_pos)
        ...    ...  # Use the child_pos_in_file for something

        :param new_base: The position that should have been the origin rather than
          (0, 0).
        :returns: The range offset relative to the base position.
        """
        if self.line_position == 0 and self.cursor_position == 0:
            return new_base
        if new_base.line_position == 0 and new_base.cursor_position == 0:
            return self
        if self.line_position == 0:
            line_number = new_base.line_position
            line_char_offset = new_base.cursor_position + self.cursor_position
        else:
            line_number = self.line_position + new_base.line_position
            line_char_offset = self.cursor_position
        return Position(
            line_number,
            line_char_offset,
        )


@dataclasses.dataclass(frozen=True, **_DATA_CLASS_OPTIONAL_ARGS)
class Range:
    """Describes a range inside a file

    This can be useful to describe things like "from line 4, cursor position 2
    to line 7 to cursor position 10". When describing a full line including the
    newline, use line N, cursor position 0 to line N+1. cursor position 0.

    It is also used to denote the size of objects (in that case, the start position
    is set to START_POSITION as a convention if the precise location is not
    specified).

    This is modelled after the "Range" in Language Server Protocol (LSP).
    """

    start_pos: Position
    end_pos: Position

    @property
    def start_line_position(self) -> int:
        """Describes the start line position as a 0-based line number

        See start_line_number if you want a human-readable line number
        """
        return self.start_pos.line_position

    @property
    def start_cursor_position(self) -> int:
        """Describes the starting cursor position

        When this value is 0, the position is at the start of a line. When it is 1, then
        the position is between the first and the second character (etc.).
        """
        return self.start_pos.cursor_position

    @property
    def start_line_number(self) -> int:
        """The start line number as human would count it"""
        return self.start_pos.line_number

    @property
    def end_line_position(self) -> int:
        """Describes the end line position as a 0-based line number

        See end_line_number if you want a human-readable line number
        """
        return self.end_pos.line_position

    @property
    def end_line_number(self) -> int:
        """The end line number as human would count it"""
        return self.end_pos.line_number

    @property
    def end_cursor_position(self) -> int:
        """Describes the end cursor position

        When this value is 0, the position is at the start of a line. When it is 1, then
        the position is between the first and the second character (etc.).
        """
        return self.end_pos.cursor_position

    @property
    def line_count(self) -> int:
        """The number of lines (newlines) spanned by this range.

        Will be zero when the range fits inside one line.
        """
        return self.end_line_position - self.start_line_position

    @classmethod
    def between(cls, a: Position, b: Position) -> "Self":
        """Computes the range between two positions

        Unlike the constructor, this will always create a "positive" range.
        That is, the "earliest" position will always be the start position
        regardless of the order they were passed to `between`. When using
        the Range constructor, you have freedom to do "inverse" ranges
        in case that is ever useful
        """
        if a.line_position > b.line_position or (
            a.line_position == b.line_position and a.cursor_position > b.cursor_position
        ):
            # Order swap, so `a` is always the earliest position
            a, b = b, a
        return cls(
            a,
            b,
        )

    def relative_to(self, new_base: Position) -> "Range":
        """Offsets the range relative to another position

        This is useful to avoid the `position_in_file()` method by caching where
        the parents position and then for its children you use `range_in_parent()`
        plus `relative_to()` to rebase the range.

        >>> parent: Locatable = ...                   # doctest: +SKIP
        >>> children: Iterable[Locatable] = ...       # doctest: +SKIP
        >>> # This will expensive
        >>> parent_pos = parent.position_in_file()     # doctest: +SKIP
        >>> for child in children:                    # doctest: +SKIP
        ...    child_range = child.range_in_parent()
        ...    # Avoid a position_in_file() for each child
        ...    child_range_in_file = child_range.relative_to(parent_pos)
        ...    ...  # Use the child_range_in_file for something

        :param new_base: The position that should have been the origin rather than
          (0, 0).
        :returns: The range offset relative to the base position.
        """
        if new_base == START_POSITION:
            return self
        return Range(
            self.start_pos.relative_to(new_base),
            self.end_pos.relative_to(new_base),
        )

    def as_size(self) -> "Range":
        """Reduces the range to a "size"

        The returned range will always have its start position to (0, 0) and
        its end position shifted accordingly if it was not already based at
        (0, 0).

        The original range is not mutated and, if it is already at (0, 0), the
        method will just return it as-is.
        """
        if self.start_pos == START_POSITION:
            return self
        line_count = self.line_count
        if line_count:
            new_end_cursor_position = self.end_cursor_position
        else:
            delta = self.end_cursor_position - self.start_cursor_position
            new_end_cursor_position = delta
        return Range(
            START_POSITION,
            Position(
                line_count,
                new_end_cursor_position,
            ),
        )

    @classmethod
    def from_position_and_size(cls, base: Position, size: "Range") -> "Self":
        """Compute a range from a position and the size of another range

        This provides you with a range starting at the base position that has
        the same effective span as the size parameter.

        :param base: The desired starting position
        :param size: A range, which will be used as a size (that is, it will
          be reduced to a size via the `as_size()` method) for the resulting
          range
        :returns: A range at the provided base position that has the size of
          the provided range.
        """
        line_position = base.line_position
        cursor_position = base.cursor_position
        size_rebased = size.as_size()
        lines = size_rebased.line_count
        if lines:
            line_position += lines
            cursor_position = size_rebased.end_cursor_position
        else:
            delta = (
                size_rebased.end_cursor_position - size_rebased.start_cursor_position
            )
            cursor_position += delta
        return cls(
            base,
            Position(
                line_position,
                cursor_position,
            ),
        )

    @classmethod
    def from_position_and_sizes(
        cls, base: Position, sizes: Iterable["Range"]
    ) -> "Self":
        """Compute a range from a position and the size of number of ranges

        :param base: The desired starting position
        :param sizes: All the ranges that combined makes up the size of the
          desired position. Note that order can affect the end result. Particularly
          the end character offset gets reset every time a size spans a line.
        :returns: A range at the provided base position that has the size of
          the provided range.
        """
        line_position = base.line_position
        cursor_position = base.cursor_position
        for size in sizes:
            size_rebased = size.as_size()
            lines = size_rebased.line_count
            if lines:
                line_position += lines
                cursor_position = size_rebased.end_cursor_position
            else:
                delta = (
                    size_rebased.end_cursor_position
                    - size_rebased.start_cursor_position
                )
                cursor_position += delta
        return cls(
            base,
            Position(
                line_position,
                cursor_position,
            ),
        )


START_POSITION = Position(0, 0)
SECOND_CHAR_POS = Position(0, 1)
SECOND_LINE_POS = Position(1, 0)
ONE_CHAR_RANGE = Range.between(START_POSITION, SECOND_CHAR_POS)
ONE_LINE_RANGE = Range.between(START_POSITION, SECOND_LINE_POS)


class Locatable:
    __slots__ = ()

    @property
    def parent_element(self):
        # type: () -> Optional[Deb822Element]
        raise NotImplementedError

    def position_in_parent(self) -> Position:
        """The start position of this token/element inside its parent

        This is operation is generally linear to the number of "parts" (elements/tokens)
        inside the parent.
        """

        parent = self.parent_element
        if parent is None:
            raise TypeError(
                "Cannot determine the position since the object is detached"
            )
        relevant_parts = itertools.takewhile(
            lambda x: x is not self, parent.iter_parts()
        )
        span = Range.from_position_and_sizes(
            START_POSITION,
            (x.size() for x in relevant_parts),
        )
        return span.end_pos

    def range_in_parent(self) -> Range:
        """The range of this token/element inside its parent

        This is operation is generally linear to the number of "parts" (elements/tokens)
        inside the parent.
        """
        pos = self.position_in_parent()
        return Range.from_position_and_size(
            pos,
            self.size(),
        )

    def position_in_file(self) -> Position:
        """The start position of this token/element in this file

        This is an *expensive* operation and in many cases have to traverse
        the entire file structure to answer the query.  Consider whether
        you can maintain the parent's position and then use
        `position_in_parent()` combined with
        `child_position.relative_to(parent_position)`

        """
        position = self.position_in_parent()
        parent = self.parent_element
        if parent is not None:
            parent_position = parent.position_in_file()
            position = position.relative_to(parent_position)
        return position

    def size(self) -> Range:
        """Describe the objects size as a continuous range"""
        raise NotImplementedError
