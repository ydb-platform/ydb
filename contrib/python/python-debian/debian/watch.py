#!/usr/bin/python3
# Copyright (C) 2019-2020 Jelmer Vernooij <jelmer@debian.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""Functions for working with watch files."""

from __future__ import annotations


import logging
import re

from typing import (
    Iterable,
    Iterator,
    Sequence,
    TextIO,
)

# The default watch file version to use for new files.
DEFAULT_VERSION = 4

# Standard substitutions applied by uscan as documented in uscan(1):
SUBSTITUTIONS = {
    # This is substituted by the legal upstream version regex (capturing).
    '@ANY_VERSION@': r'[-_]?(\d[\-+\.:\~\da-zA-Z]*)',
    # This is substituted by the typical archive file extension regex
    # (non-capturing).
    '@ARCHIVE_EXT@': r'(?i)\.(?:tar\.xz|tar\.bz2|tar\.gz|zip|tgz|tbz|txz)',
    # This is substituted by the typical signature file extension regex
    # (non-capturing).
    '@SIGNATURE_EXT@':
        r'(?i)\.(?:tar\.xz|tar\.bz2|tar\.gz|zip|tgz|tbz|txz)'
        r'\.(?:asc|pgp|gpg|sig|sign)',
    # This is substituted by the typical Debian extension regexp (capturing).
    '@DEB_EXT@': r'[\+~](debian|dfsg|ds|deb)(\.)?(\d+)?$',
}


logger = logging.getLogger('debian.watch')


class MissingVersion(Exception):
    """The version= line is missing."""


class WatchFileFormatError(ValueError):
    """Raised when the input is not valid.
    """


def expand(text: str, package: str) -> str:
    """Apply substitutions to a string.

    :param text: text to apply substitutions to
    :param package: package name, as a string
    :return: text with subsitutions applied
    """
    substs = dict(SUBSTITUTIONS.items())
    # This is substituted with the source package name found in the first line
    # of the debian/changelog file.
    substs['@PACKAGE@'] = package
    for k, v in substs.items():
        text = text.replace(k, v)
    return text


def _complain(msg: str, strict: bool) -> None:
    if strict:
        raise WatchFileFormatError(msg)
    logger.warning(msg)


class WatchFile:
    """A Debian watch file.

    :ivar entries: list of Watch entries
    :ivar options: optional list of global options, applied to all Watch
        entries
    :ivar version: watch file version
    """

    def __init__(self,
                 entries: Sequence[Watch] | None = None,
                 options: Sequence[str] | None = None,
                 version: int | None = DEFAULT_VERSION,
                 ):
        self.version = version
        if entries is None:
            entries = []
        self.entries = entries
        if options is None:
            options = []
        self.options = options

    def __iter__(self) -> Iterator[Watch]:
        return iter(self.entries)

    def dump(self, f: TextIO) -> None:
        """Write the contents of a watch file to a file-like object.

        Note that this will not preserve the formatting of the original file,
        and thus it is currently not possible to use this function to
        parse and reserialize a file and end up with the same contents.

        :param f: File-like object to write to
        """
        def serialize_options(opts: Sequence[str]) -> str:
            s = ','.join(opts)
            if ' ' in s or '\t' in s:
                return 'opts="' + s + '"'
            return 'opts=' + s
        if self.version is not None:
            f.write('version=%d\n' % self.version)
        if self.options:
            f.write(serialize_options(self.options) + '\n')
        for entry in self.entries:
            if entry.options:
                f.write(serialize_options(entry.options) + ' ')
            f.write(entry.url)
            if entry.matching_pattern:
                f.write(' ' + entry.matching_pattern)
            if entry.version:
                f.write(' ' + entry.version)
            if entry.script:
                f.write(' ' + entry.script)
            f.write('\n')

    @classmethod
    def from_lines(cls,
                   lines: Iterable[str],
                   strict: bool = False) -> WatchFile | None:
        """Parse from the contents that make up a watch file.

        :param lines: watch file lines to parse
        :return: instance or None if there are no non-comment lines in the file
        :raise MissingVersion: if there is no version number declared
        :raise ValueError: when syntax errors are encountered
        """
        joined_lines: list[list[str]] = []
        continued: list[str] = []
        for line in lines:
            if line.startswith('#'):
                continue
            if not line.strip():
                continue
            if line.rstrip('\n').endswith('\\'):
                continued.append(line.rstrip('\n\\'))
            else:
                continued.append(line)
                joined_lines.append(continued)
                continued = []
        if continued:
            # Hmm, broken line?
            _complain('watchfile ended with \\; skipping last line', strict)
            joined_lines.append(continued)
        if not joined_lines:
            return None
        firstline = ''.join(joined_lines.pop(0))
        try:
            key, value = firstline.split('=', 1)
        except ValueError:
            raise MissingVersion()
        if key.strip() != 'version':
            raise MissingVersion()
        version = int(value.strip())
        persistent_options = []
        entries = []
        for chunked in joined_lines:
            if version > 3:
                # Leading whitespace is stripped in version
                # 4 and up.
                chunked = [chunk.lstrip() for chunk in chunked]
            line = ''.join(chunked).strip()
            if not line:
                continue
            if line.startswith('opts='):
                if line[5] == '"':
                    optend = line.index('"', 6)
                    if optend == -1:
                        raise ValueError('Not matching " in %r' % line)
                    opts_str = line[6:optend]
                    line = line[optend+1:]
                else:
                    try:
                        (opts_str, line) = line[5:].split(None, 1)
                    except ValueError:
                        opts_str = line[5:]
                        line = ''
                opts = opts_str.split(',')
            else:
                opts = []
            if line:
                try:
                    url, line = line.split(None, 1)
                except ValueError:
                    url = line
                    line = ''
                m = re.findall(r'/([^/]*\([^/]*\)[^/]*)$', url)
                if m:
                    parts = (str(m[0]), ) + tuple(line.split(None, 1))
                    url = url[:-len(m[0])-1]
                else:
                    parts = tuple(line.split(None, 2))
                entries.append(Watch(url, *parts, opts=opts))  # type: ignore
            else:
                persistent_options.extend(opts)
        return cls(
            entries=entries, options=persistent_options, version=version)


class Watch:
    """Watch line entry.

    This will contain the attributes documented in uscan(1):

    :ivar url: The URL (possibly including the filename regex)
    :ivar matching_pattern: a filename regex, optional
    :ivar version: version policy, optional
    :ivar script: script to run, optional
    :ivar opts: a list of options, as strings
    """

    def __init__(self,
                 url: str,
                 matching_pattern: str | None = None,
                 version: str | None = None,
                 script: str | None = None,
                 opts: Sequence[str] | None = None,
                 ):
        self.url = url
        self.matching_pattern = matching_pattern
        self.version = version
        self.script = script
        if opts is None:
            opts = []
        self.options = opts

    def __repr__(self) -> str:
        return (
            "{}({!r}, matching_pattern={!r}, version={!r}, script={!r}, opts={!r})".format(
                self.__class__.__name__, self.url, self.matching_pattern,
                self.version, self.script, self.options))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Watch):
            return False
        return (other.url == self.url and
                other.matching_pattern == self.matching_pattern and
                other.version == self.version and
                other.script == self.script and
                other.options == self.options)
