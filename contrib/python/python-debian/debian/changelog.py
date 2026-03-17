""" Facilities for reading and writing Debian changelogs

The aim of this module is to provide programmatic access to Debian changelogs
to query and manipulate them. The format for the changelog is defined in
`deb-changelog(5)
<https://manpages.debian.org/stretch/dpkg-dev/deb-changelog.5.html>`_

Stability: The API is not marked as stable but hasn't changed incompatibly
since 2007. Potential users of these classes are asked to work with the
`python-debian` maintainers to improve, extend and stabilise this API.

Overview
========

Create a changelog object using the constuctor. Pass it the contents of the
file if there are some entries, or ``None`` to create an empty changelog::

    >>> import debian.changelog
    >>> ch = debian.changelog.Changelog()
    >>> maintainer, email = 'John Doe', 'joe@example.com'
    >>> timestamp = 1617222715
    >>> # You might want to use get_maintainer() a la:
    >>> # maintainer, email = debian.changelog.get_maintainer()
    >>> ch.new_block(
    ...     package='example',
    ...     version='0.1',
    ...     distributions='unstable',
    ...     urgency='low',
    ...     author="%s <%s>" % (maintainer, email),
    ...     # You can also omit timestamp, if you are fine with "now"
    ...     # We use a hard-coded timestamp for deterministic output
    ...     date=debian.changelog.format_date(timestamp=1617222715, localtime=False)
    ... )
    >>> ch.add_change('')
    >>> ch.add_change(' * Some change')
    >>> ch.add_change('')
    >>> print(ch, end='')
    example (0.1) unstable; urgency=low
    <BLANKLINE>
     * Some change
    <BLANKLINE>
     -- John Doe <joe@example.com>  Wed, 31 Mar 2021 20:31:55 -0000


If you have the full contents of a changelog, but are only interested in the
most recent versions you can pass the ``max_blocks`` keyword parameter to the
constuctor to limit the number of blocks of the changelog that will be parsed.
If you are only interested in the most recent version of the package then pass
``max_blocks=1``::

    >>> import gzip
    >>> from debian.changelog import Changelog
    >>> with gzip.open('/usr/share/doc/dpkg/changelog.Debian.gz') as fh:  # doctest: +SKIP
    ...     ch = Changelog(fh, max_blocks=1)
    >>> print('''
    ...     Package: %s
    ...     Version: %s
    ...     Urgency: %s''' % (ch.package, ch.version, ch.urgency))  # doctest: +SKIP
        Package: dpkg
        Version: 1.18.24
        Urgency: medium


See `/usr/share/doc/python-debian/examples/changelog/` or the
`git repository
<https://salsa.debian.org/python-debian-team/python-debian/tree/master/
examples/changelog>`_
for examples of usage.


The :class:`Changelog` class is the key class within this module.

Changelog Classes
-----------------
"""

# Copyright (C) 2006-2007  James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008       Canonical Ltd.
# Copyright (C) 2018-2023  Stuart Prescott <stuart@debian.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

# The parsing code is based on that from dpkg which is:
# Copyright 1996 Ian Jackson
# Copyright 2005 Frank Lichtenheld <frank@lichtenheld.de>
# and licensed under the same license as above.

from __future__ import annotations

import email.utils
import logging
import os
import re
import socket

from typing import (
    Iterable,
    Iterator,
    IO,
    Pattern,
    Union,
)

# pwd is only available on Unix platforms.
try:
    import pwd
except ImportError:
    pass

from debian.debian_support import Version


IterableDataSource = Union[
    bytes,
    str,
    IO[str],
    Iterable[str],
    Iterable[bytes],
]

logger = logging.getLogger('debian.changelog')


class ChangelogParseError(Exception):
    """Indicates that the changelog could not be parsed"""
    is_user_error = True

    def __init__(self, line: str) -> None:
        self._line = line
        super().__init__()

    def __str__(self) -> str:
        return "Could not parse changelog: "+self._line


class ChangelogCreateError(Exception):
    """Indicates that changelog could not be created, as all the information
    required was not given"""


class VersionError(Exception):
    """Indicates that the version does not conform to the required format"""

    is_user_error = True

    def __init__(self, version: str) -> None:
        self._version = version
        super().__init__()

    def __str__(self) -> str:
        return "Could not parse version: " + self._version


class ChangeBlock:
    """Holds all the information about one block from the changelog.

    See `deb-changelog(5)
    <https://manpages.debian.org/stretch/dpkg-dev/deb-changelog.5.html>`_
    for more details about the format of the changelog block and the
    necessary data.

    :param package: str, name of the package
    :param version: str or Version, version of the package
    :param distributions: str, distributions to which the package is
        released
    :param urgency: str, urgency of the upload
    :param urgency_comment: str, comment about the urgency setting
    :param changes: list of str, individual changelog entries for this
        block
    :param author: str, name and email address of the changelog author
    :param date: str, date of the changelog in RFC822 (`date -R`) format
    :param other_pairs: dict, key=value pairs from the header of the
        changelog, other than the urgency value that is specified
        separately
    :param encoding: specify the encoding to be used; note that Debian
        Policy mandates the use of UTF-8.
    """

    def __init__(self,
                 package: str | None = None,
                 version: Version | str | None = None,
                 distributions: str | None = None,
                 urgency: str | None = None,
                 urgency_comment: str | None = None,
                 changes: list[str] | None = None,
                 author: str | None = None,
                 date: str | None = None,
                 other_pairs: dict[str, str] | None = None,
                 encoding: str = 'utf-8',
                ) -> None:
        self._raw_version: str | None = None
        self._set_version(version)
        self.package = package
        self.distributions = distributions
        self.urgency = urgency or "unknown"
        self.urgency_comment = urgency_comment or ''
        self._changes: list[str] = changes or []
        self.author = author
        self.date = date
        self._trailing: list[str] = []
        self.other_pairs = other_pairs or {}
        self._encoding = encoding
        self._no_trailer = False
        self._trailer_separator = "  "

    def _get_version(self) -> Version | None:
        if self._raw_version is None:
            return None
        return Version(self._raw_version)

    def _set_version(self, version: Version | str | None) -> None:
        if version is not None:
            self._raw_version = str(version)
        else:
            self._raw_version = None

    # need old property() syntax until mypy can type check getters and setters
    # properly https://github.com/python/mypy/issues/3004
    version = property(
        _get_version, _set_version,
        doc="The package version that this block pertains to"
    )

    def other_keys_normalised(self) -> dict[str, str]:
        """ Obtain a dict from the block header (other than urgency) """
        norm_dict = {}
        for (key, value) in self.other_pairs.items():
            key = key[0].upper() + key[1:].lower()
            m = xbcs_re.match(key)
            if m is None:
                key = "XS-%s" % key
            norm_dict[key] = value
        return norm_dict

    def changes(self) -> list[str]:
        """ Get the changelog entries for this block as a list of str """
        return self._changes

    def add_trailing_line(self, line: str) -> None:
        """ Add a sign-off (trailer) line to the block """
        self._trailing.append(line)

    def add_change(self, change: str) -> None:
        """ Append a change entry to the block """
        if not self._changes:
            self._changes = [change]
        else:
            # Bit of trickery to keep the formatting nicer with a blank
            # line at the end if there is one
            changes = self._changes
            changes.reverse()
            added = False
            for i, ch_entry in enumerate(changes):
                if ch_entry and not ch_entry.isspace():
                    changes.insert(i, change)
                    added = True
                    break
            changes.reverse()
            if not added:
                changes.append(change)
            self._changes = changes

    def _get_bugs_closed_generic(self, type_re: Pattern[str]) -> list[int]:
        changes = ' '.join(self._changes)
        bugs = []
        for match in type_re.finditer(changes):
            closes_list = match.group(0)
            for bugmatch in re.finditer(r"\d+", closes_list):
                bugs.append(int(bugmatch.group(0)))
        return bugs

    @property
    def bugs_closed(self) -> list[int]:
        """ List of (Debian) bugs closed by the block """
        return self._get_bugs_closed_generic(closes)

    @property
    def lp_bugs_closed(self) -> list[int]:
        """ List of Launchpad bugs closed by the block """
        return self._get_bugs_closed_generic(closeslp)

    def _format(self, allow_missing_author: bool | None = False) -> str:
        # TODO(jsw): Switch to StringIO or a list to join at the end.
        block = ""
        if self.package is None:
            raise ChangelogCreateError("Package not specified")
        block += self.package + " "
        if self._raw_version is None:
            raise ChangelogCreateError("Version not specified")
        block += "(" + self._raw_version + ") "
        if self.distributions is None:
            raise ChangelogCreateError("Distribution not specified")
        block += self.distributions + "; "
        if self.urgency is None:
            raise ChangelogCreateError("Urgency not specified")
        block += "urgency=" + self.urgency + self.urgency_comment
        for (key, value) in self.other_pairs.items():
            block += f", {key}={value}"
        block += '\n'
        if self.changes() is None:
            raise ChangelogCreateError("Changes not specified")
        for change in self.changes():
            block += change + "\n"
        if not self._no_trailer:
            block += " --"
            if self.author is not None:
                block += " " + self.author
            elif not allow_missing_author:
                raise ChangelogCreateError("Author not specified")
            if self.date is not None:
                block += self._trailer_separator + self.date
            elif not allow_missing_author:
                raise ChangelogCreateError("Date not specified")
            block += "\n"
        for line in self._trailing:
            block += line + "\n"
        return block

    def __str__(self) -> str:
        return self._format()

    def __bytes__(self) -> bytes:
        # pylint: disable=invalid-bytes-returned
        # pylint bug https://github.com/PyCQA/pylint/issues/3599
        return str(self).encode(self._encoding)


topline = re.compile(
    r'^(\w%(name_chars)s*) \(([^\(\) \t]+)\)'
    r'((\s+%(name_chars)s+)+)\;'
    % {'name_chars': '[-+0-9a-z.]'},
    re.IGNORECASE)
changere = re.compile(r'^\s\s+.*$')
endline = re.compile(
    r'^ -- (.*) <(.*)>(  ?)((\w+\,\s*)?\d{1,2}\s+\w+\s+'
    r'\d{4}\s+\d{1,2}:\d\d:\d\d\s+[-+]\d{4}\s*)$')
endline_nodetails = re.compile(
    r'^ --(?: (.*) <(.*)>(  ?)((\w+\,\s*)?\d{1,2}'
    r'\s+\w+\s+\d{4}\s+\d{1,2}:\d\d:\d\d\s+[-+]\d{4}'
    r'))?\s*$')
maintainerre = re.compile(r"^(.*)\s+<(.*)>$")
keyvalue = re.compile(r'^([-0-9a-z]+)=\s*(.*\S)$', re.IGNORECASE)
value_re = re.compile(r'^([-0-9a-z]+)((\s+.*)?)$', re.IGNORECASE)
xbcs_re = re.compile('^X[BCS]+-', re.IGNORECASE)
emacs_variables = re.compile(r'^(;;\s*)?Local variables:', re.IGNORECASE)
vim_variables = re.compile('^vim:', re.IGNORECASE)
cvs_keyword = re.compile(r'^\$\w+:.*\$')
comments = re.compile(r'^\# ')
more_comments = re.compile(r'^/\*.*\*/')
closes = re.compile(
    r'closes:\s*(?:bug)?\#?\s?\d+(?:,\s*(?:bug)?\#?\s?\d+)*',
    re.IGNORECASE)
closeslp = re.compile(r'lp:\s+\#\d+(?:,\s*\#\d+)*', re.IGNORECASE)

old_format_re1 = re.compile(
    r'^(\w+\s+\w+\s+\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}'
    r'\s+[\w\s]*\d{4})\s+(.*)\s+(<|\()(.*)(\)|>)')
old_format_re2 = re.compile(
    r'^(\w+\s+\w+\s+\d{1,2},?\s*\d{4})\s+(.*)'
    r'\s+(<|\()(.*)(\)|>)')
old_format_re3 = re.compile(
    r'^(\w[-+0-9a-z.]*) \(([^\(\) \t]+)\)\;?',
    re.IGNORECASE)
old_format_re4 = re.compile(
    r'^([\w.+-]+)(-| )(\S+) Debian (\S+)',
    re.IGNORECASE)
old_format_re5 = re.compile(
    '^Changes from version (.*) to (.*):',
    re.IGNORECASE)
old_format_re6 = re.compile(
    r'^Changes for [\w.+-]+-[\w.+-]+:?\s*$',
    re.IGNORECASE)
old_format_re7 = re.compile(r'^Old Changelog:\s*$', re.IGNORECASE)
old_format_re8 = re.compile(r'^(?:\d+:)?\w[\w.+~-]*:?\s*$')


class Changelog:
    """Represents a debian/changelog file.

    To get the properly formatted changelog back out of the object
    merely call `str()` on it. The returned string should be a properly
    formatted changelog.

    :param file: str, list of str, or file-like.
        The contents of the changelog, either as a ``str``, ``unicode`` object,
        or an iterator of lines such as a filehandle, (each line is either a
        ``str`` or ``unicode``)
    :param max_blocks: int, optional (Default: ``None``, no limit)
        The maximum number of blocks to parse from the input.
    :param allow_empty_author: bool, optional (Default: `False`),
        Whether to allow an empty author in the trailer line of a change
        block.
    :param strict: bool, optional (Default: ``False``, use a warning)
        Whether to raise an exception if there are errors.
    :param encoding: str,
        If the input is a str or iterator of str, the encoding to use when
        interpreting the input.

    There are a number of errors that may be thrown by the module:

    - :class:`ChangelogParseError`:
      Indicates that the changelog could not be parsed, i.e. there is a line
      that does not conform to the requirements, or a line was found out of
      its normal position. May be thrown when using the method
      `parse_changelog`.
      The constructor will not throw this exception.
    - :class:`ChangelogCreateError`:
      Some information required to create the changelog was not available.
      This can be thrown when `str()` is used on the object, and will occur
      if a required value is `None`.
    - :class:`VersionError`:
      The string used to create a Version object cannot be parsed as it
      doesn't conform to the specification of a version number. Can be
      thrown when creating a Changelog object from an existing changelog,
      or instantiating a Version object directly to assign to the version
      attribute of a Changelog object.

    If you have a changelog that may have no author information yet as
    it is still a work in progress, i.e. the author line is just::

        --

    rather than::

        -- Author <author@debian.org>  Thu, 12 Dec 2006 12:23:34 +0000

    then you can pass ``allow_empty_author=True`` to the Changelog
    constructor. If you do this then the ``author`` and ``date``
    attributes may be ``None``.

    """

    # TODO(jsw): Avoid masking the 'file' built-in.
    def __init__(self,
                 file: IterableDataSource | None = None,
                 max_blocks: int | None = None,
                 allow_empty_author: bool = False,
                 strict: bool = False,
                 encoding: str = 'utf-8',
                 ) -> None:
        self._encoding = encoding
        self._blocks: list[ChangeBlock] = []
        self.initial_blank_lines: list[str] = []
        if file is not None:
            self.parse_changelog(
                file, max_blocks=max_blocks,
                allow_empty_author=allow_empty_author,
                strict=strict)

    @staticmethod
    def _parse_error(message: str, strict: bool) -> None:
        if strict:
            raise ChangelogParseError(message)
        logger.warning(message)

    def parse_changelog(self,
                        file: IterableDataSource | None,
                        max_blocks: int | None = None,
                        allow_empty_author: bool = False,
                        strict: bool = True,
                        encoding: str | None = None,
                       ) -> None:
        """ Read and parse a changelog file

        If you create an Changelog object without specifying a changelog
        file, you can parse a changelog file with this method. If the
        changelog doesn't parse cleanly, a :class:`ChangelogParseError`
        exception is thrown. The constructor will parse the changelog on
        a best effort basis.
        """
        first_heading = "first heading"
        next_heading_or_eof = "next heading of EOF"
        start_of_change_data = "start of change data"
        more_changes_or_trailer = "more change data or trailer"
        slurp_to_end = "slurp to end"

        encoding = encoding or self._encoding

        if file is None:
            self._parse_error('Empty changelog file.', strict)
            return

        self._blocks = []
        self.initial_blank_lines = []

        current_block = ChangeBlock(encoding=encoding)
        changes = []

        state = first_heading
        old_state = None
        if isinstance(file, bytes):
            file = file.decode(encoding)
        if isinstance(file, str):
            # Make sure the changelog file is not empty.
            if not file.strip():
                self._parse_error('Empty changelog file.', strict)
                return

            file = file.splitlines()
        for line in file:
            if not isinstance(line, str):
                line = line.decode(encoding)
            # Support both lists of lines without the trailing newline and
            # those with trailing newlines (e.g. when given a file object
            # directly)
            line = line.rstrip('\n')
            if state in (first_heading, next_heading_or_eof):
                top_match = topline.match(line)
                if top_match is not None:
                    if (max_blocks is not None
                            and len(self._blocks) >= max_blocks):
                        return
                    current_block.package = top_match.group(1)
                    current_block._raw_version = top_match.group(2)
                    current_block.distributions = top_match.group(3).lstrip()

                    pairs = line.split(";", 1)[1]
                    all_keys: dict[str, str] = {}
                    other_pairs: dict[str, str] = {}
                    for pair in pairs.split(','):
                        pair = pair.strip()
                        kv_match = keyvalue.match(pair)
                        if kv_match is None:
                            self._parse_error(
                                "Invalid key-value pair after ';': %s" % pair,
                                strict)
                            continue
                        key = kv_match.group(1)
                        key_lower = key.lower()
                        value = kv_match.group(2)
                        if key_lower in all_keys:
                            self._parse_error(
                                "Repeated key-value: "
                                "%s" % key_lower, strict)
                        all_keys[key_lower] = value
                        if key_lower == "urgency":
                            val_match = value_re.match(value)
                            if val_match is None:
                                self._parse_error(
                                    "Badly formatted urgency value: %s" %
                                    value, strict)
                            else:
                                current_block.urgency = val_match.group(1)
                                comment = val_match.group(2)
                                if comment is not None:
                                    current_block.urgency_comment = comment
                        else:
                            other_pairs[key] = value
                    current_block.other_pairs = other_pairs
                    state = start_of_change_data
                elif not line or line.isspace():
                    if state == first_heading:
                        self.initial_blank_lines.append(line)
                    else:
                        self._blocks[-1].add_trailing_line(line)
                else:
                    emacs_match = emacs_variables.match(line)
                    vim_match = vim_variables.match(line)
                    cvs_match = cvs_keyword.match(line)
                    comments_match = comments.match(line)
                    more_comments_match = more_comments.match(line)
                    if ((emacs_match is not None or vim_match is not None)
                            and state != first_heading):
                        self._blocks[-1].add_trailing_line(line)
                        old_state = state
                        state = slurp_to_end
                        continue
                    if (cvs_match is not None or comments_match is not None
                            or more_comments_match is not None):
                        if state == first_heading:
                            self.initial_blank_lines.append(line)
                        else:
                            self._blocks[-1].add_trailing_line(line)
                        continue
                    if ((old_format_re1.match(line) is not None
                         or old_format_re2.match(line) is not None
                         or old_format_re3.match(line) is not None
                         or old_format_re4.match(line) is not None
                         or old_format_re5.match(line) is not None
                         or old_format_re6.match(line) is not None
                         or old_format_re7.match(line) is not None
                         or old_format_re8.match(line) is not None)
                            and state != first_heading):
                        self._blocks[-1].add_trailing_line(line)
                        old_state = state
                        state = slurp_to_end
                        continue
                    self._parse_error(
                        "Unexpected line while looking for %s: %s" %
                        (state, line), strict)
                    if state == first_heading:
                        self.initial_blank_lines.append(line)
                    else:
                        self._blocks[-1].add_trailing_line(line)
            elif state in (start_of_change_data, more_changes_or_trailer):
                change_match = changere.match(line)
                end_match = endline.match(line)
                if change_match is not None:
                    changes.append(line)
                    state = more_changes_or_trailer
                elif end_match is not None:
                    if end_match.group(3) != '  ':
                        self._parse_error(
                            "Badly formatted trailer line: %s" % line, strict)
                        current_block._trailer_separator = end_match.group(3)
                    current_block.author = "%s <%s>" \
                        % (end_match.group(1), end_match.group(2))
                    current_block.date = end_match.group(4)
                    current_block._changes = changes
                    self._blocks.append(current_block)
                    changes = []
                    current_block = ChangeBlock(encoding=encoding)
                    state = next_heading_or_eof
                elif endline_nodetails.match(line) is not None:
                    if not allow_empty_author:
                        self._parse_error(
                            "Badly formatted trailer line: %s" % line, strict)
                        continue
                    current_block._changes = changes
                    self._blocks.append(current_block)
                    changes = []
                    current_block = ChangeBlock(encoding=encoding)
                    state = next_heading_or_eof
                elif not line or line.isspace():
                    changes.append(line)
                else:
                    cvs_match = cvs_keyword.match(line)
                    comments_match = comments.match(line)
                    more_comments_match = more_comments.match(line)
                    if (cvs_match is not None or comments_match is not None
                            or more_comments_match is not None):
                        changes.append(line)
                        continue
                    self._parse_error(
                        "Unexpected line while looking for %s: %s" %
                        (state, line), strict)
                    changes.append(line)
            elif state == slurp_to_end:
                if old_state == next_heading_or_eof:
                    self._blocks[-1].add_trailing_line(line)
                else:
                    changes.append(line)
            else:
                assert False, "Unknown state: %s" % state

        if (state not in (next_heading_or_eof, slurp_to_end)
                or (state == slurp_to_end
                    and old_state != next_heading_or_eof)):
            self._parse_error(
                "Found eof where expected %s" % state, strict)
            current_block._changes = changes
            current_block._no_trailer = True
            self._blocks.append(current_block)

    def get_version(self) -> Version | None:
        """Return a Version object for the last version"""
        return self._blocks[0].version   # type: ignore

    def set_version(self, version: Version | str) -> None:
        """Set the version of the last changelog block

        version can be a full version string, or a Version object
        """
        self._blocks[0].version = Version(version)

    # Use old property() syntax until mypy can separately type getter and
    # setter. https://github.com/python/mypy/issues/3004
    version = property(
        get_version, set_version,
        doc="""Version object for latest changelog block.
            (Property that can both get and set the version.)"""
    )

    # For convenience, let's expose some of the version properties
    full_version = property(
        lambda self: self.version.full_version,
        doc="The full version number of the last version"
    )
    epoch = property(
        lambda self: self.version.epoch,
        doc="The epoch number of the last revision, or `None` "
        "if no epoch was used."
    )
    debian_version = property(
        lambda self: self.version.debian_revision,
        doc="The debian part of the version number of the last version."
    )
    debian_revision = property(
        lambda self: self.version.debian_revision,
        doc="The debian part of the version number of the last version."
    )
    upstream_version = property(
        lambda self: self.version.upstream_version,
        doc="The upstream part of the version number of the last version."
    )

    def get_package(self) -> str | None:
        """Returns the name of the package in the last entry."""
        return self._blocks[0].package

    def set_package(self, package: str) -> None:
        """ set the name of the package in the last entry. """
        self._blocks[0].package = package

    package = property(
        get_package, set_package,
        doc="Name of the package in the last version"
    )

    def get_versions(self) -> list[Version]:
        return self.versions

    @property
    def versions(self) -> list[Version]:
        """Returns a list of :class:`debian.debian_support.Version` objects
        that are listed in the changelog."""
        return [block.version for block in self._blocks]

    def _raw_versions(self) -> list[str | None]:
        return [block._raw_version for block in self._blocks]

    def _format(self, allow_missing_author: bool | None = False) -> str:
        pieces = []
        for line in self.initial_blank_lines:
            pieces.append(line + '\n')
        for block in self._blocks:
            pieces.append(block._format(allow_missing_author=allow_missing_author))
        return ''.join(pieces)

    def __str__(self) -> str:
        return self._format()

    def __bytes__(self) -> bytes:
        # pylint: disable=invalid-bytes-returned
        # pylint bug https://github.com/PyCQA/pylint/issues/3599
        return str(self).encode(self._encoding)

    def __iter__(self) -> Iterator[ChangeBlock]:
        return iter(self._blocks)

    def __getitem__(self, n: Version | int | str) -> ChangeBlock:
        """ select a changelog entry by number, version string, or Version

        :param n: integer or str representing a version or Version object
        """
        if isinstance(n, str):
            return self[Version(n)]
        if isinstance(n, int):
            idx = n
        else:   # a Version object
            idx = self.versions.index(n)
        return self._blocks[idx]

    def __len__(self) -> int:
        return len(self._blocks)

    def set_distributions(self, distributions: str) -> None:
        self._blocks[0].distributions = distributions

    distributions = property(
        lambda self: self._blocks[0].distributions, set_distributions,
        doc="""\
A string indicating the distributions that the package will be uploaded to
in the most recent version."""
    )

    def set_urgency(self, urgency: str) -> None:
        self._blocks[0].urgency = urgency

    urgency = property(
        lambda self: self._blocks[0].urgency, set_urgency,
        doc="""\
A string indicating the urgency with which the most recent version will
be uploaded."""
    )

    def add_change(self, change: str) -> None:
        """ and a new dot point to a changelog entry

        Adds a change entry to the most recent version. The change entry
        should conform to the required format of the changelog (i.e. start
        with two spaces). No line wrapping or anything will be performed,
        so it is advisable to do this yourself if it is a long entry. The
        change will be appended to the current changes, no support is
        provided for per-maintainer changes.
        """
        self._blocks[0].add_change(change)

    def set_author(self, author: str) -> None:
        """ set the author of the top changelog entry """
        self._blocks[0].author = author

    author = property(
        lambda self: self._blocks[0].author, set_author,
        doc="""\
        The author of the most recent change.
        This should be a properly formatted name/email pair."""
    )

    def set_date(self, date: str) -> None:
        """ set the date of the top changelog entry

        :param date: str
            a properly formatted date string (`date -R` format; see Policy)
        """
        self._blocks[0].date = date

    date = property(
        lambda self: self._blocks[0].date, set_date,
        doc="""\
        The date associated with the current entry.
        Should be a properly formatted string with the date and timezone.
        See the :func:`format_date()` function."""
    )

    def new_block(self,
                  package: str | None = None,
                  version: Version | str | None = None,
                  distributions: str | None = None,
                  urgency: str | None = None,
                  urgency_comment: str | None = None,
                  changes: list[str] | None = None,
                  author: str | None = None,
                  date: str | None = None,
                  other_pairs: dict[str, str] | None = None,
                  encoding: str | None = None,
                  ) -> None:
        """ Add a new changelog block to the changelog

        Start a new :class:`ChangeBlock` entry representing a new version
        of the package. The arguments (all optional) are passed directly
        to the :class:`ChangeBlock` constructor; they specify the values
        that can be provided to the `set_*` methods of this class. If
        they are omitted the associated attributes *must* be assigned to
        before the changelog is formatted as a str or written to a file.
        """
        encoding = encoding or self._encoding
        block = ChangeBlock(package, version, distributions,
                            urgency, urgency_comment,
                            changes, author, date, other_pairs, encoding)
        if self._blocks:
            # #998715 - only add a trailing line if there are other blocks.
            block.add_trailing_line('')
        self._blocks.insert(0, block)

    def write_to_open_file(self, filehandle: IO[str]) -> None:
        """ Write the changelog entry to a filehandle

        Write the changelog out to the filehandle passed. The file argument
        must be an open file object.
        """
        filehandle.write(str(self))


def get_maintainer() -> tuple[str | None, str | None]:
    """Get the maintainer information in the same manner as dch.

    This function gets the information about the current user for
    the maintainer field using environment variables of gecos
    information as appropriate.

    It uses the same algorithm as dch to get the information, namely
    DEBEMAIL, DEBFULLNAME, EMAIL, NAME, /etc/mailname and gecos.

    :returns: a tuple of the full name, email pair as strings.
        Either of the pair may be None if that value couldn't
        be determined.
    """
    env = os.environ

    # Split email and name
    if 'DEBEMAIL' in env:
        match_obj = maintainerre.match(env['DEBEMAIL'])
        if match_obj:
            if 'DEBFULLNAME' not in env:
                env['DEBFULLNAME'] = match_obj.group(1)
            env['DEBEMAIL'] = match_obj.group(2)
    if 'DEBEMAIL' not in env or 'DEBFULLNAME' not in env:
        if 'EMAIL' in env:
            match_obj = maintainerre.match(env['EMAIL'])
            if match_obj:
                if 'DEBFULLNAME' not in env:
                    env['DEBFULLNAME'] = match_obj.group(1)
                env['EMAIL'] = match_obj.group(2)

    # Get maintainer's name
    maintainer: str | None = None
    if 'DEBFULLNAME' in env:
        maintainer = env['DEBFULLNAME']
    elif 'NAME' in env:
        maintainer = env['NAME']
    else:
        # Use password database if no data in environment variables
        try:
            maintainer = re.sub(r',.*', '', pwd.getpwuid(os.getuid()).pw_gecos)
        # pwd might not be imported giving a NameError
        except (KeyError, AttributeError, NameError):
            pass

    # Get maintainer's mail address
    email_address: str | None = None
    if 'DEBEMAIL' in env:
        email_address = env['DEBEMAIL']
    elif 'EMAIL' in env:
        email_address = env['EMAIL']
    else:
        addr = None
        if os.path.exists('/etc/mailname'):
            with open('/etc/mailname', encoding="UTF-8") as f:
                addr = f.readline().strip()
        if not addr:
            addr = socket.getfqdn()
        if addr:
            try:
                user = pwd.getpwuid(os.getuid()).pw_name
            # pwd might not be imported giving a NameError
            except (AttributeError, NameError):
                addr = None
            else:
                if not user:
                    addr = None
                else:
                    addr = f"{user}@{addr}"

        if addr:
            email_address = addr

    return (maintainer, email_address)


def format_date(timestamp: float | None = None, localtime: bool = True) -> str:
    """ format a datestamp in the required format for the changelog

    :param timestamp: float, optional. The timestamp (seconds since epoch)
        for which the date string should be created. If not specified, the
        current time is used.
    :param localtime: bool, optional (default True). Use the local timezone
        in the date string.

    :returns: str, date stamp formatted according to the changelog
        specification (i.e. RFC822).
    """
    return email.utils.formatdate(timestamp, localtime)
