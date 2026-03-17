"""parsing and generation of content lines"""

import re

from icalendar.parser.parameter import Parameters
from icalendar.parser.property import unescape_backslash, unescape_list_or_string
from icalendar.parser.string import (
    escape_string,
    foldline,
    unescape_string,
    validate_token,
)
from icalendar.parser_tools import DEFAULT_ENCODING, ICAL_TYPE, to_unicode

UFOLD = re.compile("(\r?\n)+[ \t]")
NEWLINE = re.compile(r"\r?\n")


class Contentline(str):
    """A content line is basically a string that can be folded and parsed into
    parts.
    """

    __slots__ = ("strict",)

    def __new__(cls, value, strict=False, encoding=DEFAULT_ENCODING):
        value = to_unicode(value, encoding=encoding)
        assert "\n" not in value, (
            "Content line can not contain unescaped new line characters."
        )
        self = super().__new__(cls, value)
        self.strict = strict
        return self

    @classmethod
    def from_parts(
        cls,
        name: ICAL_TYPE,
        params: Parameters,
        values,
        sorted: bool = True,  # noqa: A002
    ):
        """Turn a parts into a content line."""
        assert isinstance(params, Parameters)
        if hasattr(values, "to_ical"):
            values = values.to_ical()
        else:
            from icalendar.prop import vText

            values = vText(values).to_ical()
        # elif isinstance(values, basestring):
        #    values = escape_char(values)

        # TODO: after unicode only, remove this
        # Convert back to unicode, after to_ical encoded it.
        name = to_unicode(name)
        values = to_unicode(values)
        if params:
            params = to_unicode(params.to_ical(sorted=sorted))
            if params:
                # some parameter values can be skipped during serialization
                return cls(f"{name};{params}:{values}")
        return cls(f"{name}:{values}")

    def parts(self) -> tuple[str, Parameters, str]:
        """Split the content line into ``name``, ``parameters``, and ``values`` parts.

        Properly handles escaping with backslashes and double-quote sections
        to avoid corrupting URL-encoded characters in values.

        Example with parameter:

        .. code-block:: text

            DESCRIPTION;ALTREP="cid:part1.0001@example.org":The Fall'98 Wild

        Example without parameters:

        .. code-block:: text

            DESCRIPTION:The Fall'98 Wild
        """
        try:
            name_split: int | None = None
            value_split: int | None = None
            in_quotes: bool = False
            escaped: bool = False

            for i, ch in enumerate(self):
                if ch == '"' and not escaped:
                    in_quotes = not in_quotes
                elif ch == "\\" and not in_quotes:
                    escaped = True
                    continue
                elif not in_quotes and not escaped:
                    # Find first delimiter for name
                    if ch in ":;" and name_split is None:
                        name_split = i
                    # Find value delimiter (first colon)
                    if ch == ":" and value_split is None:
                        value_split = i

                escaped = False

            # Validate parsing results
            if not value_split:
                # No colon found - value is empty, use end of string
                value_split = len(self)

            # Extract name - if no delimiter,
            #   take whole string for validate_token to reject
            name = self[:name_split] if name_split else self
            validate_token(name)

            if not name_split or name_split + 1 == value_split:
                # No delimiter or empty parameter section
                raise ValueError("Invalid content line")  # noqa: TRY301
            # Parse parameters - they still need to be escaped/unescaped
            # for proper handling of commas, semicolons, etc. in parameter values
            param_str = escape_string(self[name_split + 1 : value_split])
            params = Parameters.from_ical(param_str, strict=self.strict)
            params = Parameters(
                (unescape_string(key), unescape_list_or_string(value))
                for key, value in iter(params.items())
            )
            # Unescape backslash sequences in values but preserve URL encoding
            values = unescape_backslash(self[value_split + 1 :])
        except ValueError as exc:
            raise ValueError(
                f"Content line could not be parsed into parts: '{self}': {exc}"
            ) from exc
        return (name, params, values)

    @classmethod
    def from_ical(cls, ical, strict=False):
        """Unfold the content lines in an iCalendar into long content lines."""
        ical = to_unicode(ical)
        # a fold is carriage return followed by either a space or a tab
        return cls(UFOLD.sub("", ical), strict=strict)

    def to_ical(self):
        """Long content lines are folded so they are less than 75 characters
        wide.
        """
        return foldline(self).encode(DEFAULT_ENCODING)


class Contentlines(list):
    """I assume that iCalendar files generally are a few kilobytes in size.
    Then this should be efficient. for Huge files, an iterator should probably
    be used instead.
    """

    def to_ical(self):
        """Simply join self."""
        return b"\r\n".join(line.to_ical() for line in self if line) + b"\r\n"

    @classmethod
    def from_ical(cls, st):
        """Parses a string into content lines."""
        st = to_unicode(st)
        try:
            # a fold is carriage return followed by either a space or a tab
            unfolded = UFOLD.sub("", st)
            lines = cls(Contentline(line) for line in NEWLINE.split(unfolded) if line)
            lines.append("")  # '\r\n' at the end of every content line
        except Exception as e:
            raise ValueError("Expected StringType with content lines") from e
        return lines


__all__ = ["Contentline", "Contentlines"]
