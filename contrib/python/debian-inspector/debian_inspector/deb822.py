#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/debian-inspector for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

"""
A parser for deb822 data files as used by Debian control and copyright files,
and several related formats.

For details, see:
- https://www.debian.org/doc/debian-policy/ch-controlfields
- https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
- https://datatracker.ietf.org/doc/rfc2822/

Why yet another Debian 822 parser?

This module exists because no existing parser module supports some must-have
features. Neither the standard email module nor the python-debian library, nor
other existing libraries can do these:

- track the start and end line numbers of each field within each paragraph: we
  need line numbers to trace license detection in copyright files.

- lenient parsing recovering from common errors: this helps handling a larger
  variety of copyright files even if they are not exactly well-formed
"""

import re

import attr

from debian_inspector.debcon import read_text_file


def get_paragraphs_as_field_groups(text):
    """
    Yield lists of Deb822Field for each paragraph in a ``text`` each separated
    by one or more empty lines. Raise Exceptions on errors.
    """
    return get_paragraphs_as_field_groups_from_lines(NumberedLine.lines_from_text(text))


def get_paragraphs_as_field_groups_from_file(location):
    """
    Yield lists of Deb822Field for each paragraph in a control file at
    ``location``. Raise Exceptions on errors.
    """
    if not location:
        return []
    return get_paragraphs_as_field_groups(read_text_file(location))


def get_paragraphs_as_field_groups_from_lines(numbered_lines):
    """
    Yield lists of Deb822Field for each paragraph in a ``numbered_lines`` list
    of NumberedLine. Raise Exceptions on errors.
    """
    numbered_lines = list(numbered_lines)
    last_idx = len(numbered_lines) - 1
    fields_group = []
    current_field = None
    for idx, line in enumerate(numbered_lines):
        # blank line: One or more blank line should terminates paragraph (e.g. a
        # fields_group) and starts a new one. There is one exception
        # though which is when the next line is not a new field declaration.
        # While this is not valid, this is a common mistake in copyright files
        # and we want to recover from this so we treat that empty line as a
        # continuation.
        if line.is_blank():
            # peek one line ahead if next is a header field...
            if (
                current_field
                and idx != last_idx
                and not numbered_lines[idx + 1].is_field_declaration()
                and not numbered_lines[idx + 1].is_blank()
            ):
                current_field.add_continuation_line(line)
                continue

            if fields_group:
                fields_group = clean_fields(fields_group)
                yield fields_group
                fields_group = []
                current_field = None
            else:
                # skip empty lines in between paragraphs
                pass

        # continuation line: append this to the current field
        elif current_field and line.is_field_continuation():
            current_field.add_continuation_line(line)

        # new field declaration line: create a new field
        elif line.is_field_declaration():
            current_field = Deb822Field.from_line(line)
            if not current_field:
                raise Exception(f"Invalid field line: {line}")
            fields_group.append(current_field)

        # an unknown line: we yield the curremt group and then yield this as
        # its own group
        else:
            if fields_group:
                fields_group = clean_fields(fields_group)
                yield fields_group

            # craft a synthetic header with name "unknown"
            yield [Deb822Field(name="unknown", lines=[line])]
            fields_group = []
            current_field = None

    # last header fields group: if any: yield this
    if fields_group:
        fields_group = clean_fields(fields_group)
        yield fields_group


def clean_fields(fields):
    """
    Clean and return a ``fields`` list of Deb822Field.
    """
    for hf in fields or []:
        hf.rstrip()
    return fields


is_field_declaration = re.compile(r"^[a-z]+[a-z0-9\-]*:.*$", re.IGNORECASE).match
is_field_continuation = re.compile(r"^[ \t]+[\S]+.*$", re.IGNORECASE).match


@attr.s(slots=True)
class NumberedLine:
    """
    A text line that tracks its absolute line number. Numbers start at 1.
    """

    number = attr.ib()
    value = attr.ib()

    def is_blank(self):
        return not self.value.strip()

    def is_field_declaration(self):
        """
        Return True if this is a continuation line.

        For example:

        >>> NumberedLine(1, 'foo').is_field_declaration()
        False
        >>> NumberedLine(1, '').is_field_declaration()
        False
        >>> NumberedLine(1, '   ').is_field_declaration()
        False
        >>> NumberedLine(1, '  Some: bar ').is_field_declaration()
        False
        >>> NumberedLine(1, 'Some:').is_field_declaration()
        True
        >>> NumberedLine(1, 'foo: bar').is_field_declaration()
        True
        >>> NumberedLine(1, 'foo:bar').is_field_declaration()
        True
        >>> NumberedLine(1, 'foo: ').is_field_declaration()
        True
        >>> NumberedLine(1, ' .').is_field_declaration()
        False
        >>> NumberedLine(1, '    .').is_field_declaration()
        False
        """
        return bool(is_field_declaration(self.value))

    def is_field_continuation(self):
        """
        Return True if this is a field declaration line.

        For example:

        >>> NumberedLine(1, 'foo').is_field_continuation()
        False
                >>> NumberedLine(1, '').is_field_continuation()
                False
                >>> NumberedLine(1, '   ').is_field_continuation()
                False
                >>> NumberedLine(1, '	').is_field_continuation()
                False
        >>> NumberedLine(1, '   foo').is_field_continuation()
        True
        >>> NumberedLine(1, ' .').is_field_continuation()
        True
        >>> NumberedLine(1, '	.').is_field_continuation()
        True
        >>> NumberedLine(1, ' "').is_field_continuation()
        True
        >>> NumberedLine(1, ' (').is_field_continuation()
        True
        """
        return bool(is_field_continuation(self.value))

    @classmethod
    def lines_from_text(cls, text):
        """
        Return a list of Line from a ``text``
        """
        return [
            cls(number=number, value=value)
            for number, value in enumerate(text.splitlines(False), 1)
        ]

    def to_dict(self):
        return attr.asdict(self)


@attr.s(slots=True)
class Deb822Field:
    """
    A Deb822Field field with a name and a list of NumberedLines.
    """

    # field name, normalized as stripped and lowercase
    name = attr.ib(default=None)

    lines = attr.ib(default=attr.Factory(list))

    @property
    def text(self):
        return "\n".join(l.value for l in self.lines)

    @property
    def start_line(self):
        return self.lines[0].number

    @property
    def end_line(self):
        return self.lines[-1].number

    def rstrip(self):
        """
        Remove the last lines of these lines they are all blank or empty.
        Return self.
        """
        lines = self.lines
        while lines:
            if not lines[-1].value.strip():
                lines.pop()
            else:
                break
        return self

    def add_continuation_line(self, line):
        self.lines.append(NumberedLine(number=line.number, value=line.value.rstrip()))

    @classmethod
    def from_line(cls, line):
        """
        Parse a ``line`` Line object as a "Name: value" declaration line and
        return a Deb822Field object. Return None if this is not a deb822 field
        declaration line.
        """
        if not line or not line.is_field_declaration():
            return

        name, _colon, value = line.value.partition(":")
        name = name.strip().lower()
        if not name:
            return

        # There are some cases where Debian copyright files spells the
        # license field of a paragraph as "licence". We must correct this
        # otherwise the license information will be stored under "licence"
        # instead of "license".
        if name == "licence":
            name = "license"

        value = value.strip()
        first_line = NumberedLine(number=line.number, value=value)
        return cls(name=name, lines=[first_line])

    def to_dict(self):
        return attr.asdict(self)
