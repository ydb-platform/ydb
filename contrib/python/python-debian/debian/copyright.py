"""Utilities for parsing and creating machine-readable debian/copyright files.

The specification for the format (also known as DEP5) is available here:
https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/

Start from the Copyright docstring for usage information.

Copyright Classes
-----------------
"""

# Copyright (C) 2014       Google, Inc.
# Copyright (C) 2018-2023  Stuart Prescott <stuart@debian.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation, either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

from __future__ import annotations

import collections
import itertools
import logging
import io
import re

from typing import (
    Any,
    IO,
    Iterable,
    Iterator,
    Pattern,
    Union,
    cast,
)

from debian._deb822_repro import (
    parse_deb822_file,
    Deb822ParagraphElement,
    Deb822FileElement, Deb822NoDuplicateFieldsParagraphElement,
    SyntaxOrParseError,
)
from debian.deb822 import RestrictedField, RestrictedFieldError

try:
    # Typing only
    from debian.deb822 import Deb822ValueType
except ImportError:
    pass


ParagraphTypes = Union["FilesParagraph", "LicenseParagraph"]
AllParagraphTypes = Union["Header", "FilesParagraph", "LicenseParagraph"]


_CURRENT_FORMAT = (
    'https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/')

_KNOWN_FORMATS = frozenset([
    _CURRENT_FORMAT,
])


logger = logging.getLogger(__name__)


class Error(Exception):
    """Base class for exceptions in this module."""


class NotMachineReadableError(Error):
    """Raised when the input is not a machine-readable debian/copyright file."""


class MachineReadableFormatError(Error, ValueError):
    """Raised when the input is not valid.

    This is both a `copyright.Error` and a `ValueError` to ease handling of
    errors coming from this module.
    """


def _complain(msg: str, strict: bool) -> None:
    if strict:
        raise MachineReadableFormatError(msg)
    logger.warning(msg)


class Copyright:
    """Represents a debian/copyright file.

    A Copyright object contains a Header paragraph and a list of additional
    Files or License paragraphs.  It provides methods to iterate over those
    paragraphs, in addition to adding new ones.  It also provides a mechanism
    for finding the Files paragraph (if any) that matches a particular
    filename.

    Typical usage::

        with io.open('debian/copyright', 'rt', encoding='utf-8') as f:
            c = copyright.Copyright(f)

            header = c.header
            # Header exposes standard fields, e.g.
            print('Upstream name: ', header.upstream_name)
            lic = header.license
            if lic:
                print('Overall license: ', lic.synopsis)
            # You can also retrieve and set custom fields.
            header['My-Special-Field'] = 'Very special'

            # Find the license for a given file.
            paragraph = c.find_files_paragraph('debian/rules')
            if paragraph:
                print('License for debian/rules: ', paragraph.license)

            # Dump the result, including changes, to another file.
            with io.open('debian/copyright.new', 'wt', encoding='utf-8') as f:
                c.dump(f=f)

    It is possible to build up a Copyright from scratch, by modifying the
    header and using add_files_paragraph and add_license_paragraph.  See the
    associated method docstrings.
    """

    def __init__(self,
                 sequence: list[str] | IO[str] | None = None,
                 encoding: str = 'utf-8',
                 strict: bool = True) -> None:
        """ Create a new copyright file in the current format.

        :param sequence: Sequence of lines, e.g. a list of strings or a
            file-like object.  If not specified, a blank Copyright object is
            initialized.
        :param encoding: Encoding to use, in case input is raw byte strings.
            It is recommended to use unicode objects everywhere instead, e.g.
            by opening files in text mode.
        :param strict: Raise if format errors are detected in the data.

        Raises:
            :class:`NotMachineReadableError` if 'sequence' does not contain a
                machine-readable debian/copyright file.
            MachineReadableFormatError if 'sequence' is not a valid file.
        """
        super().__init__()

        self.__paragraphs: list[AllParagraphTypes] = []

        if sequence is not None:
            header = None
            try:
                self.__file = parse_deb822_file(
                        sequence=sequence, encoding=encoding,
                        accept_files_with_duplicated_fields=not strict)
            except SyntaxOrParseError as e:
                raise NotMachineReadableError(str(e))
            for p in self.__file:
                if header is None:
                    header = Header(p)
                elif 'Files' in p:
                    pf = FilesParagraph(p, strict, strict=strict)
                    self.__paragraphs.append(pf)
                elif 'License' in p:
                    pl = LicenseParagraph(p, strict)
                    self.__paragraphs.append(pl)
                else:
                    _complain('Non-header paragraph has neither "Files" nor '
                              '"License" fields', strict)
            if not header:
                raise NotMachineReadableError('no paragraphs in input')
            self.__header = header

        else:
            self.__file = Deb822FileElement.new_empty_file()
            self.__header = Header()
            self.__file.append(self.__header._underlying_paragraph)
            self.__paragraphs.append(self.__header)

    @property
    def header(self) -> Header:
        """The file header paragraph."""
        return self.__header

    @header.setter
    def header(self, hdr: Header) -> None:
        if not isinstance(hdr, Header):
            raise TypeError('value must be a Header object')
        self.__header = hdr

    def all_paragraphs(self) -> Iterator[AllParagraphTypes]:
        """Returns an iterator over all paragraphs (header, Files, License).

        The header (returned first) will be returned as a Header object; file
        paragraphs as FilesParagraph objects; license paragraphs as
        LicenseParagraph objects.

        """
        return itertools.chain([self.header], (p for p in self.__paragraphs))

    def __iter__(self) -> Iterator[AllParagraphTypes]:
        """Iterate over all paragraphs

        see all_paragraphs() for more information

        """
        return self.all_paragraphs()

    def all_files_paragraphs(self) -> Iterator[FilesParagraph]:
        """Returns an iterator over the contained FilesParagraph objects."""
        return (p for p in self.__paragraphs if isinstance(p, FilesParagraph))

    def find_files_paragraph(self, filename: str) -> FilesParagraph | None:
        """Returns the FilesParagraph for the given filename.

        In accordance with the spec, this method returns the last FilesParagraph
        that matches the filename.  If no paragraphs matched, returns None.
        """
        result = None
        for p in self.all_files_paragraphs():
            if p.matches(filename):
                result = p
        return result

    def add_files_paragraph(self, paragraph: FilesParagraph) -> None:
        """Adds a FilesParagraph to this object.

        The paragraph is inserted directly after the last FilesParagraph (which
        might be before a standalone LicenseParagraph).
        """
        if not isinstance(paragraph, FilesParagraph):
            raise TypeError('paragraph must be a FilesParagraph instance')

        last_i = -1
        for i, p in enumerate(self.__paragraphs):
            if isinstance(p, FilesParagraph):
                last_i = i
        self.__paragraphs.insert(last_i + 1, paragraph)
        self.__file.insert(last_i + 2, paragraph._underlying_paragraph)

    def all_license_paragraphs(self) -> Iterator[LicenseParagraph]:
        """Returns an iterator over standalone LicenseParagraph objects."""
        return (p for p in self.__paragraphs if isinstance(p, LicenseParagraph))

    def add_license_paragraph(self, paragraph: LicenseParagraph) -> None:
        """Adds a LicenceParagraph to this object.

        The paragraph is inserted after any other paragraphs.
        """
        if not isinstance(paragraph, LicenseParagraph):
            raise TypeError('paragraph must be a LicenseParagraph instance')
        self.__paragraphs.append(paragraph)
        self.__file.append(paragraph._underlying_paragraph)

    def dump(self, f: IO[str] | None = None) -> str | None:
        """Dumps the contents of the copyright file.

        If f is None, returns a unicode object.  Otherwise, writes the contents
        to f, which must be a file-like object that is opened in text mode
        (i.e. that accepts unicode objects directly).  It is thus up to the
        caller to arrange for the file to do any appropriate encoding.
        """
        # TODO(jelmer): Write bytes
        s = self.__file.dump()
        if f is not None:
            f.write(s)
            return None
        return s


def _single_line(s: str) -> str:
    """Returns s if it is a single line; otherwise raises MachineReadableFormatError."""
    if '\n' in s:
        raise MachineReadableFormatError('must be single line')
    return s


class _LineBased:
    """Namespace for conversion methods for line-based lists as tuples."""
    # TODO(jsw): Expose this somewhere else?  It may have more general utility.

    @staticmethod
    def from_str(s: str | None) -> Iterable[str]:
        """Returns the lines in 's', with whitespace stripped, as a tuple."""
        return tuple(v for v in
                     (line.strip() for line in (s or '').strip().splitlines())
                     if v)

    @staticmethod
    def to_str(seq: Iterable[str]) -> str | None:
        """Returns the sequence as a string with each element on its own line.

        If 'seq' has one element, the result will be on a single line.
        Otherwise, the first line will be blank.
        """
        l = list(seq)
        if not l:
            return None

        def process_and_validate(s: str) -> str:
            s = s.strip()
            if not s:
                raise MachineReadableFormatError('values must not be empty')
            if '\n' in s:
                raise MachineReadableFormatError(
                    'values must not contain newlines')
            return s

        if len(l) == 1:
            return process_and_validate(l[0])

        tmp = ['']
        for s in l:
            tmp.append(' ' + process_and_validate(s))
        return '\n'.join(tmp)


class _SpaceSeparated:
    """Namespace for conversion methods for space-separated lists as tuples."""
    # TODO(jsw): Expose this somewhere else?  It may have more general utility.

    _has_space = re.compile(r'\s')

    @staticmethod
    def from_str(s: str | None) -> Iterable[str]:
        """Returns the values in s as a tuple (empty if only whitespace)."""
        return tuple(v for v in (s or '').split() if v)

    @classmethod
    def to_str(cls, seq: Iterable[str]) -> str | None:
        """Returns the sequence as a space-separated string (None if empty)."""
        l = list(seq)
        if not l:
            return None
        tmp = []
        for s in l:
            if cls._has_space.search(s):
                raise MachineReadableFormatError(
                    'values must not contain whitespace')
            s = s.strip()
            if not s:
                raise MachineReadableFormatError('values must not be empty')
            tmp.append(s)
        return ' '.join(tmp)


# TODO(jsw): Move multiline formatting/parsing elsewhere?

def format_multiline(s: str | None) -> str | None:
    """Formats multiline text for insertion in a Deb822ParagraphElement field.

    Each line except for the first one is prefixed with a single space.  Lines
    that are blank or only whitespace are replaced with ' .'
    """
    if s is None:
        return None
    return format_multiline_lines(s.splitlines())


def format_multiline_lines(lines: list[str]) -> str:
    """Same as format_multline, but taking input pre-split into lines."""
    out_lines = []
    for i, line in enumerate(lines):
        if i != 0:
            if not line.strip():
                line = '.'
            line = ' ' + line
        out_lines.append(line)
    return '\n'.join(out_lines)


def parse_multiline(s: str | None) -> str | None:
    """Inverse of format_multiline.

    Technically it can't be a perfect inverse, since format_multline must
    replace all-whitespace lines with ' .'.  Specifically, this function:

      - Does nothing to the first line
      - Removes first character (which must be ' ') from each proceeding line.
      - Replaces any line that is '.' with an empty line.
    """
    if s is None:
        return None
    return '\n'.join(parse_multiline_as_lines(s))


def parse_multiline_as_lines(s: str) -> list[str]:
    """Same as parse_multiline, but returns a list of lines.

    (This is the inverse of format_multiline_lines.)
    """
    lines = s.splitlines()
    for i, line in enumerate(lines):
        if i == 0:
            continue
        if line.startswith(' '):
            line = line[1:]
        else:
            raise MachineReadableFormatError(
                'continued line must begin with " "')
        if line == '.':
            line = ''
        lines[i] = line
    return lines


class License(collections.namedtuple('License', 'synopsis text')):
    """Represents the contents of a License field.  Immutable."""

    def __new__(cls, synopsis: str, text: str | None = '') -> License:
        """Creates a new License object.

        :param synopsis: The short name of the license, or an expression giving
            alternatives.  (The first line of a License field.)
        :param text: The full text of the license, if any (may be None).  The
            lines should not be mangled for "deb822"-style wrapping - i.e. they
            should not have whitespace prefixes or single '.' for empty lines.
        """
        return super().__new__(
            cls, synopsis=_single_line(synopsis), text=(text or ''))

    @classmethod
    def from_str(cls, s: str | None) -> License | None:
        if s is None:
            return None

        lines = parse_multiline_as_lines(s)
        if not lines:
            return cls('')
        return cls(lines[0], text='\n'.join(itertools.islice(lines, 1, None)))

    def to_str(self) -> str:
        return format_multiline_lines([self.synopsis] + self.text.splitlines())

    # TODO(jsw): Parse the synopsis?
    # TODO(jsw): Provide methods to look up license text for known licenses?


def globs_to_re(globs: Iterable[str]) -> Pattern[str]:
    r"""Returns an re object for the given globs.

    Only * and ? wildcards are supported.  Literal * and ? may be matched via
    \* and \?, respectively.  A literal backslash is matched \\.  Any other
    character after a backslash is forbidden.

    Empty globs match nothing.

    The pattern should be used with the `re.fullmatch` function to provide
    anchoring.

    Raises MachineReadableFormatError if any of the globs is illegal.
    """
    buf = io.StringIO()
    for i, glob in enumerate(globs):
        if i != 0:
            buf.write('|')
        i = 0
        n = len(glob)
        while i < n:
            c = glob[i]
            i += 1
            if c == '*':
                buf.write('.*')
            elif c == '?':
                buf.write('.')
            elif c == '\\':
                if i < n:
                    c = glob[i]
                    i += 1
                else:
                    raise MachineReadableFormatError(
                        'single backslash not allowed at end')
                if c in r'\?*':
                    buf.write(re.escape(c))
                else:
                    raise MachineReadableFormatError(
                        r'invalid escape sequence: \%s' % c)
            else:
                buf.write(re.escape(c))

    return re.compile(buf.getvalue(), re.MULTILINE | re.DOTALL)


class _ClassInitMeta(type):
    """Metaclass for classes that can be initialized at creation time.

    Implement the method::

      @classmethod
      def _class_init(cls, new_attrs):
          pass

    on a class, and apply this metaclass to it.  The _class_init method will be
    called right after the class is created.  The 'new_attrs' param is a dict
    containing the attributes added in the definition of the class.
    """

    def __init__(cls: Any,
                 name: Any,
                 bases: Any,
                 attrs: Any,
                 ):
        # type (...) -> None
        super().__init__(name, bases, attrs)
        cls._class_init(attrs)


class _RestrictedWrapper(metaclass=_ClassInitMeta):
    """Base class to wrap a Deb822 object, restricting write access to some keys.

    The underlying data is hidden internally.  Subclasses may keep a reference
    to the data before giving it to this class's constructor, if necessary, but
    RestrictedField should cover most use-cases.  The dump method from
    Deb822 is directly proxied.

    Typical usage::

        class Foo:
            def __init__(self, ...):
                # ...

            @staticmethod
            def from_str(self, s):
                # Parse s...
                return Foo(...)

            def to_str(self):
                # Return in string format.
                return ...

        class MyClass(deb822._RestrictedWrapper):
            def __init__(self):
                data = Deb822ParagraphElement.new_empty_paragraph()
                data['Bar'] = 'baz'
                super(MyClass, self).__init__(data)

            foo = deb822.RestrictedField(
                    'Foo', from_str=Foo.from_str, to_str=Foo.to_str)

            bar = deb822.RestrictedField('Bar', allow_none=False)

        d = MyClass()
        d['Bar'] # returns 'baz'
        d['Bar'] = 'quux' # raises RestrictedFieldError
        d.bar = 'quux'
        d.bar # returns 'quux'
        d['Bar'] # returns 'quux'

        d.foo = Foo(...)
        d['Foo'] # returns string representation of foo
    """

    __restricted_fields: frozenset[str] = frozenset()

    @classmethod
    def _class_init(cls, new_attrs):  # type: ignore
        restricted_fields = []
        for attr_name, val in new_attrs.items():
            if isinstance(val, RestrictedField):
                restricted_fields.append(val.name.lower())
                cls.__init_restricted_field(attr_name, val)  # type: ignore
        cls.__restricted_fields = frozenset(restricted_fields)

    @classmethod
    def __init_restricted_field(cls, attr_name, field):  # type: ignore
        def getter(self: _RestrictedWrapper) -> Deb822ValueType:
            val = self.__data.get(field.name)
            if field.from_str is not None:
                return field.from_str(val)
            return val

        def setter(self: _RestrictedWrapper, val: Deb822ValueType) -> None:
            if val is not None and field.to_str is not None:
                val = field.to_str(val)
            if val is None:
                if field.allow_none:
                    if field.name in self.__data:
                        del self.__data[field.name]
                else:
                    raise TypeError('value must not be None')
            else:
                self.__data[field.name] = val

        setattr(cls, attr_name, property(getter, setter, None, field.name))

    def __init__(self,
                 data: Deb822ParagraphElement,
                 _internal_validate: bool = True) -> None:
        """Initializes the wrapper over 'data', a Deb822ParagraphElement object."""
        super().__init__()
        if _internal_validate and not isinstance(data, Deb822NoDuplicateFieldsParagraphElement):
            raise ValueError("Paragraph has duplicated fields: " + str(data.__class__.__qualname__))
        self.__data: Deb822ParagraphElement = data

    @property
    def _underlying_paragraph(self) -> Deb822ParagraphElement:
        return self.__data

    def __getitem__(self, key: str) -> Deb822ValueType:
        return self.__data[key]

    def __setitem__(self, key: str, value: Deb822ValueType) -> None:
        if key.lower() in self.__restricted_fields:
            raise RestrictedFieldError(
                '%s may not be modified directly; use the associated'
                ' property' % key)
        self.__data[key] = value

    def __delitem__(self, key: str) -> None:
        if key.lower() in self.__restricted_fields:
            raise RestrictedFieldError(
                '%s may not be modified directly; use the associated'
                ' property' % key)
        del self.__data[key]

    def __iter__(self) -> Iterable[str]:
        return (str(k) for k in self.__data)

    def __len__(self) -> int:
        return len(self.__data)

    def dump(self,
             fd: IO[str] | IO[bytes] | None = None,
             encoding: str | None = None,
             text_mode: bool = False,
             ) -> str | None:
        """Calls dump() on the underlying data object.

        See Deb822.dump for more information.
        """
        if fd is not None:
            if encoding is None and not text_mode:
                self.__data.dump(cast('IO[bytes]', fd))
                return None
            # Compat with Deb822's dump
            as_str = self.__data.dump()
            if encoding is not None:
                cast('IO[bytes]', fd).write(as_str.encode(encoding))
            elif text_mode:
                cast('IO[str]', fd).write(as_str)
            return None
        return self.__data.dump()


class FilesParagraph(_RestrictedWrapper):
    """Represents a Files paragraph of a debian/copyright file.

    This kind of paragraph is used to specify the copyright and license for a
    particular set of files in the package.
    """

    _default_re = re.compile('')

    def __init__(self,
                 data: Deb822ParagraphElement,
                 _internal_validate: bool = True,
                 strict: bool = True) -> None:
        super().__init__(data, _internal_validate)

        if _internal_validate:
            if 'Files' not in data:
                raise MachineReadableFormatError('"Files" field required')
            if 'Copyright' not in data:
                _complain('Files paragraph missing Copyright field', strict)
            if 'License' not in data:
                _complain('Files paragraph missing License field', strict)

            if not self.files:
                _complain('Files paragraph has empty Files field', strict)

        self.__cached_files_pat: tuple[str, Pattern[str]] = ('', self._default_re)

    @classmethod
    def create(cls,
               files: list[str] | None,
               copyright: str | None,
               license: License | None,
              ) -> FilesParagraph:
        """Create a new FilesParagraph from its required parts.

        :param files: The list of file globs.
        :param copyright: The copyright for the files (free-form text).
        :param license: The Licence for the files.
        """
        # pylint: disable=redefined-builtin
        p = cls(Deb822ParagraphElement.new_empty_paragraph(), _internal_validate=False)
        # mypy doesn't handle the metaprogrammed properties at all
        p.files = files          # type: ignore
        p.copyright = copyright  # type: ignore
        p.license = license      # type: ignore
        return p

    def files_pattern(self) -> Pattern[str] | None:
        """Returns a regular expression equivalent to the Files globs.

        Caches the result until files is set to a different value.

        Raises ValueError if any of the globs are invalid.
        """
        files_str = self['files']
        if self.__cached_files_pat[0] != files_str:
            self.__cached_files_pat = (files_str, globs_to_re(self.files))
        return self.__cached_files_pat[1]

    def matches(self, filename: str) -> bool:
        """Returns True iff filename is matched by a glob in Files."""
        pat = self.files_pattern()
        if pat is None:
            return False
        return pat.fullmatch(filename) is not None

    files = RestrictedField(
        'Files', from_str=_SpaceSeparated.from_str,
        to_str=_SpaceSeparated.to_str, allow_none=False)

    copyright = RestrictedField('Copyright', allow_none=False)

    license = RestrictedField(
        'License', from_str=License.from_str, to_str=License.to_str,
        allow_none=False)

    comment = RestrictedField('Comment')


class LicenseParagraph(_RestrictedWrapper):
    """Represents a standalone license paragraph of a debian/copyright file.

    Minimally, this kind of paragraph requires a 'License' field and has no
    'Files' field.  It is used to give a short name to a license text, which
    can be referred to from the header or files paragraphs.
    """

    def __init__(self,
                 data: Deb822ParagraphElement,
                 _internal_validate: bool = True) -> None:
        super().__init__(data, _internal_validate)
        if _internal_validate:
            if 'License' not in data:
                raise MachineReadableFormatError('"License" field required')
            if 'Files' in data:
                raise MachineReadableFormatError(
                    'input appears to be a Files paragraph')

    @classmethod
    def create(cls, license: License) -> LicenseParagraph:
        """Returns a LicenseParagraph with the given license."""
        # pylint: disable=redefined-builtin
        if not isinstance(license, License):
            raise TypeError('license must be a License instance')
        paragraph = cls(Deb822ParagraphElement.new_empty_paragraph(), _internal_validate=False)
        paragraph.license = license   # type: ignore  ## properties
        return paragraph

    # TODO(jsw): Validate that the synopsis of the license is a short name or
    # short name with exceptions (not an alternatives expression).  This
    # requires help from the License class.
    license = RestrictedField(
        'License', from_str=License.from_str, to_str=License.to_str,
        allow_none=False)

    comment = RestrictedField('Comment')

    # Hide 'Files'.
    __files = RestrictedField('Files')


class Header(_RestrictedWrapper):
    """Represents the header paragraph of a debian/copyright file.

    Property values are all immutable, such that in order to modify them you
    must explicitly set them (rather than modifying a returned reference).
    """

    def __init__(self, data: Deb822ParagraphElement | None = None) -> None:
        """Initializer.

        :param data: A Deb822ParagraphElement object for underlying data.  If None, a
            new one will be created.
        """
        if data is None:
            data = Deb822ParagraphElement.new_empty_paragraph()
            data['Format'] = _CURRENT_FORMAT

        if 'Format-Specification' in data:
            logger.warning('use of deprecated "Format-Specification" field;'
                           ' rewriting as "Format"')
            data['Format'] = data['Format-Specification']
            del data['Format-Specification']

        super().__init__(data)

        fmt = ''  # Set this to be a string type to appease later checking
        fmt = self.format   # type: ignore
        if fmt != _CURRENT_FORMAT and fmt is not None:
            # Add a terminal slash onto the end if missing
            if not fmt.endswith('/'):
                fmt += "/"

            # Upgrade http to https if that is valid
            if fmt.startswith('http:'):
                fmt = "https:%s" % fmt[5:]

            if fmt in _KNOWN_FORMATS:
                logger.warning('Fixing Format URL')
                self.format = fmt   # type: ignore

        if fmt is None:
            raise NotMachineReadableError(
                'input is not a machine-readable debian/copyright')
        if fmt not in _KNOWN_FORMATS:
            logger.warning('format not known: %r', fmt)

    def known_format(self) -> bool:
        """Returns True iff the format is known."""
        return self.format in _KNOWN_FORMATS   # type: ignore

    def current_format(self) -> bool:
        """Returns True iff the format is the current format."""
        return self.format == _CURRENT_FORMAT   # type: ignore

    # lots of type ignores due to  https://github.com/python/mypy/issues/1279
    format = RestrictedField(
        'Format', to_str=_single_line, allow_none=False)

    upstream_name = RestrictedField(
        'Upstream-Name', to_str=_single_line)

    upstream_contact = RestrictedField(
        'Upstream-Contact', from_str=_LineBased.from_str,
        to_str=_LineBased.to_str)

    source = RestrictedField('Source')

    disclaimer = RestrictedField('Disclaimer')

    comment = RestrictedField('Comment')

    license = RestrictedField(
        'License', from_str=License.from_str, to_str=License.to_str)

    copyright = RestrictedField('Copyright')

    files_excluded = RestrictedField(
        'Files-Excluded', from_str=_LineBased.from_str,
        to_str=_LineBased.to_str)

    files_included = RestrictedField(
        'Files-Included', from_str=_LineBased.from_str,
        to_str=_LineBased.to_str)
