""" Dictionary-like interfaces to RFC822-like files

The Python deb822 aims to provide a dict-like interface to various RFC822-like
Debian data formats, like Packages/Sources, .changes/.dsc, pdiff Index files,
etc. As well as the generic :class:`Deb822` class, the specialised versions
of these classes  (:class:`Packages`, :class:`Sources`, :class:`Changes` etc)
know about special fields that contain specially formatted data such as
dependency lists or whitespace separated sub-fields.

This module has few external dependencies, but can use python-apt if available
to parse the data, which gives a very significant performance boost when
iterating over big Packages files.

Whitespace separated data within fields are known as "multifields".
The "Files" field in Sources files, for instance, has three subfields, while
"Files" in .changes files, has five; the relevant classes here know this and
correctly handle these cases.

Key lookup in Deb822 objects and their multifields is case-insensitive, but
the original case is always preserved, for example when printing the object.

The Debian project and individual developers make extensive use of GPG
signatures including in-line signatures. GPG signatures are automatically
detected, verified and the payload then offered to the parser.

Relevant documentation on the Deb822 file formats available here.

- `deb-control(5)
  <https://manpages.debian.org/unstable/dpkg-dev/deb-control.5.html>`_,
  the `control` file in the binary package (generated from
  `debian/control` in the source package)
- `deb-changes(5)
  <https://manpages.debian.org/unstable/dpkg-dev/deb-changes.5.html>`_,
  `changes` files that developers upload to add new packages to the
  archive.
- `dsc(5) <https://manpages.debian.org/unstable/dpkg-dev/dsc.5.html>`_,
  Debian Source Control file that defines the files that are part of a
  source package.
- `Debian mirror format <http://wiki.debian.org/RepositoryFormat>`_,
  including documentation for Packages, Sources files etc.

Overview of deb822 Classes
--------------------------

Classes that deb822 provides:

  * :class:`Deb822` base class with no multifields. A Deb822 object holds a
    single entry from a Deb822-style file, where paragraphs are separated by
    blank lines and there may be many paragraphs within a file. The
    :func:`~Deb822.iter_paragraphs` function yields paragraphs from a data
    source.

  * :class:`Packages` represents a Packages file from a Debian mirror.
    It extends the Deb822 class by interpreting fields that
    are package relationships (Depends, Recommends etc). Iteration is forced
    through python-apt for performance and conformance.

  * :class:`Dsc` represents .dsc files (Debian Source Control) that are the
    metadata file of the source package.

    Multivalued fields:

      * Files: md5sum, size, name
      * Checksums-Sha1: sha1, size, name
      * Checksums-Sha256: sha256, size, name
      * Checksums-Sha512: sha512, size, name
      * Package-List: package, package-type, section, priority, _other

  * :class:`Sources` represents a Sources file from a Debian mirror.
    It extends the Dsc class by interpreting fields that
    are package relationships (Build-Depends, Build-Conflicts etc).
    Iteration is forced through python-apt for performance and conformance.

  * :class:`Release` represents a Release file from a Debian mirror.

    Multivalued fields:

      * MD5Sum: md5sum, size, name
      * SHA1: sha1, size, name
      * SHA256: sha256, size, name
      * SHA512: sha512, size, name

  * :class:`Changes` represents a .changes file that is uploaded to "change
    the archive" by including new source or binary packages.

    Multivalued fields:

      * Files: md5sum, size, section, priority, name
      * Checksums-Sha1: sha1, size, name
      * Checksums-Sha256: sha256, size, name
      * Checksums-Sha512: sha512, size, name

  * :class:`PdiffIndex` represents a pdiff Index file (`foo`.diff/Index) file
    from a Debian mirror.

    Multivalued fields:

      * SHA1-Current: SHA1, size
      * SHA1-History: SHA1, size, date
      * SHA1-Patches: SHA1, size, date
      * SHA1-Download: SHA1, size, filename
      * X-Unmerged-SHA1-History: SHA1, size, date
      * X-Unmerged-SHA1-Patches: SHA1, size, date
      * X-Unmerged-SHA1-Download: SHA1, size, filename
      * SHA256-Current: SHA256, size
      * SHA256-History: SHA256, size, date
      * SHA256-Patches: SHA256, size, date
      * SHA256-Download: SHA256, size, filename
      * X-Unmerged-SHA256-History: SHA256, size, date
      * X-Unmerged-SHA256-Patches: SHA256, size, date
      * X-Unmerged-SHA256-Download: SHA256, size, filename

  * :class:`Removals` represents the ftp-master removals file listing when
    and why source and binary packages are removed from the archive.


Input
=====

Deb822 objects are normally initialized from a file object (from which
at most one paragraph is read), a multiline string of Deb822 format, a
string representing a filename, or a pathlib.Path object.
Alternatively, any sequence that returns one line of input at a time
may be used, e.g a list of strings.

PGP signatures, if present, will be stripped.

Example::

    >>> from debian.deb822 import Deb822
    >>> filename = '/var/lib/apt/lists/deb.debian.org_debian_dists_sid_InRelease'
    >>> with open(filename) as fh: # doctest: +SKIP
    ...     rel = Deb822(fh)
    >>> print('Origin: {Origin}\\nCodename: {Codename}\\nDate: {Date}'.format_map( # doctest: +SKIP
    ...       rel))
    Origin: Debian
    Codename: sid
    Date: Sat, 07 Apr 2018 14:41:12 UTC
    >>> print(list(rel.keys()))   # doctest: +SKIP
    ['Origin', 'Label', 'Suite', 'Codename', 'Changelogs', 'Date',
    'Valid-Until', 'Acquire-By-Hash', 'Architectures', 'Components',
    'Description', 'MD5Sum', 'SHA256']


In the above, the `MD5Sum` and `SHA256` fields are just a very long string. If
instead the :class:`Release` class is used, these fields are interpreted and
can be addressed::

    >>> from debian.deb822 import Release
    >>> filename = '/var/lib/apt/lists/deb.debian.org_debian_dists_sid_InRelease'
    >>> with open(filename) as fh: # doctest: +SKIP
    ...     rel = Release(fh)
    >>> wanted = 'main/binary-amd64/Packages'
    >>> [(l['sha256'], l['size']) for l in rel['SHA256'] if l['name'] == wanted]   # doctest: +SKIP
    [('c0f7aa0b92ebd6971c0b64f93f52a8b2e15b0b818413ca13438c50eb82586665', '45314424')]


Iteration
=========

All classes use the :func:`~Deb822.iter_paragraphs` class method to easily
iterate through each paragraph in a file that contains multiple entries
(e.g. a Packages.gz file).
For example::

    >>> with open('/mirror/debian/dists/sid/main/binary-i386/Sources') as f:  # doctest: +SKIP
    ...     for src in Sources.iter_paragraphs(f):
    ...         print(src['Package'], src['Version'])

The `iter_paragraphs` methods can use python-apt if available to parse
the data, since it significantly boosts performance.
If python-apt is not present and the
file is a compressed file, it must first be decompressed manually. Note that
python-apt should not be used on `debian/control` files since python-apt is
designed to be strict and fast while the syntax of `debian/control` is a
superset of what python-apt is designed to parse.
This function is overridden to force use of the
python-apt parser using `use_apt_pkg` in the :func:`~Packages.iter_paragraphs`
and :func:`~Sources.iter_paragraphs` functions.


Sample usage
============

Manipulating a .dsc file::

   from debian import deb822

   with open('foo_1.1.dsc') as f:
       d = deb822.Dsc(f)
   source = d['Source']
   version = d['Version']

   for f in d['Files']:
       print('Name:', f['name'])
       print('Size:', f['size'])
       print('MD5sum:', f['md5sum'])

    # If we like, we can change fields
    d['Standards-Version'] = '3.7.2'

    # And then dump the new contents
    with open('foo_1.1.dsc2', 'w') as new_dsc:
        d.dump(new_dsc)

(TODO: Improve, expand)

Deb822 Classes
--------------
"""

# Copyright (C) 2005-2006  dann frazier <dannf@dannf.org>
# Copyright (C) 2006-2010  John Wright <john@johnwright.org>
# Copyright (C) 2006       Adeodato Simó <dato@net.com.org.es>
# Copyright (C) 2008       Stefano Zacchiroli <zack@upsilon.cc>
# Copyright (C) 2014       Google, Inc.
# Copyright (C) 2014-2024  Stuart Prescott <stuart@debian.org>
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

import builtins    # pylint: disable=unused-import
from collections import defaultdict
import collections.abc
import datetime
import email.utils
import functools
import logging
import io
from pathlib import Path
import re
import subprocess
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Generator,
    Iterator,
    Iterable,
    IO,
    List,
    NamedTuple,
    Mapping,
    MutableMapping,
    overload,
    TypeVar,
    Union,
    TYPE_CHECKING,
)
import warnings

try:
    if TYPE_CHECKING:
        from typing_extensions import (
            Literal,
            Protocol,
            TypedDict,
        )
except ImportError:
    pass


# We re-export OrderedSet in case someone used it externally.  The others look
# sufficiently internal that we do not bother with backwards compatibility.
# pylint: disable=useless-import-alias
from debian._util import (
    OrderedSet as OrderedSet,
    _CaseInsensitiveString, _strI,
    default_field_sort_key,
)
import debian.debian_support
import debian.changelog


IterableInputDataType = Union[
    IO[str],
    IO[bytes],
    Iterable[str],
    Iterable[bytes],
]
InputDataType = Union[
    bytes,
    str,
    IterableInputDataType,
]

Deb822ValueType = Any    # this really is Union[str, List] but that is a can of worms
Deb822Mapping = Mapping[str, Deb822ValueType]
Deb822MutableMapping = MutableMapping[str, Deb822ValueType]
T_Deb822Dict = TypeVar('T_Deb822Dict', bound='Deb822Dict')

try:
    import apt_pkg
    # This module uses apt_pkg only for its TagFile interface.
    apt_pkg.TagFile     # pylint: disable=pointless-statement
    _have_apt_pkg = True
except (ImportError, AttributeError):
    _have_apt_pkg = False


try:
    import charset_normalizer
    _have_charset_normalizer = True
    _have_chardet = False
except ImportError:
    _have_charset_normalizer = False
    try:
        import chardet
        _have_chardet = True
    except ImportError:
        _have_chardet = False


def _has_fileno(f: Any) -> bool:
    """ test that a file-like object is really a filehandle

    Only filehandles can be given to apt_pkg.TagFile.
    """
    try:
        f.fileno()
        return True
    except (AttributeError, io.UnsupportedOperation):
        return False


# In Python 3.10, there is a default of 128. Python 3.5 requires an explicit cache size.
@functools.lru_cache(128)
def _cached_strI(v: str) -> _strI:
    return _strI(v)


GPGV_DEFAULT_KEYRINGS = frozenset(['/usr/share/keyrings/debian-keyring.gpg'])
GPGV_EXECUTABLE = '/usr/bin/gpgv'


logger = logging.getLogger('debian.deb822')


class Error(Exception):
    """Base class for custom exceptions in this module."""


class RestrictedFieldError(Error):
    """Raised when modifying the raw value of a field is not allowed."""


class MergeChangesError(Error):
    """Raised for problems merging .changes files."""


if TYPE_CHECKING:
    _TagSectionWrapper_base = Deb822Mapping
else:
    _TagSectionWrapper_base = collections.abc.Mapping


class TagSectionWrapper(_TagSectionWrapper_base):
    """Wrap a TagSection object, using its find_raw method to get field values

    This allows us to pick which whitespace to strip off the beginning and end
    of the data, so we don't lose leading newlines.
    """

    def __init__(self,
                 section: apt_pkg.TagSection[bytes],
                 decoder: _AutoDecoder | None = None,
                 ) -> None:
        self.__section = section
        self.decoder = decoder or _AutoDecoder()
        super().__init__()

    def __iter__(self) -> Iterator[str]:
        for key in self.__section.keys():
            if not key.startswith('#'):
                yield key

    def __len__(self) -> int:
        return len([key for key in self.__section.keys()
                    if not key.startswith('#')])

    def __getitem__(self, key: str) -> str:
        # find_raw may give str or bytes depending on how it goes with decoding
        # and how it is set up by the TagFile iterator
        sraw = self.__section.find_raw(key)

        s = self.decoder.decode(sraw)

        if s is None:
            raise KeyError(key)

        # Get just the stuff after the first ':'
        # Could use s.partition if we only supported python >= 2.5
        data = s[s.find(':')+1:]

        # Get rid of spaces and tabs after the ':', but not newlines, and strip
        # off any newline at the end of the data.
        return data.lstrip(' \t').rstrip('\n')


if TYPE_CHECKING:
    _Deb822Dict_base = Deb822MutableMapping
else:
    _Deb822Dict_base = collections.abc.MutableMapping


class Deb822Dict(_Deb822Dict_base):
    """A dictionary-like object suitable for storing RFC822-like data.

    Deb822Dict behaves like a normal dict, except:
        - key lookup is case-insensitive
        - key order is preserved
        - if initialized with a _parsed parameter, it will pull values from
          that dictionary-like object as needed (rather than making a copy).
          The _parsed dict is expected to be able to handle case-insensitive
          keys.

    If _parsed is not None, an optional _fields parameter specifies which keys
    in the _parsed dictionary are exposed.
    """

    # See the end of the file for the definition of _strI

    def __init__(self,
                 _dict: Deb822Mapping | Iterable[tuple[str,str]] | None = None,
                 _parsed: Deb822 | TagSectionWrapper | None = None,
                 _fields: list[str] | None = None,
                 encoding: str = "utf-8",
                ) -> None:
        self.__dict: dict[_CaseInsensitiveString, Deb822ValueType] = {}
        self.__keys = OrderedSet()
        self.__parsed: Deb822 | TagSectionWrapper | None = None
        self.encoding = encoding
        self.decoder = _AutoDecoder(self.encoding)
        super().__init__()

        if _dict is not None:
            # _dict may be a dict or a list of two-sized tuples
            # define the type in advance and then ignore the next assignments
            # https://github.com/python/mypy/issues/1424
            items: list[tuple[str,str]] = []
            if hasattr(_dict, 'items'):
                items = _dict.items()  # type: ignore
            else:
                items = list(_dict)

            try:
                for k, v in items:
                    self[k] = v
            except ValueError:
                this = len(self.__keys)
                len_ = len(items[this])
                raise ValueError(
                    'dictionary update sequence element #%d has '
                    'length %d; 2 is required' % (this, len_))

        if _parsed is not None:
            self.__parsed = _parsed
            if _fields is None:
                self.__keys.extend([_cached_strI(k) for k in self.__parsed])
            else:
                self.__keys.extend([_cached_strI(f) for f in _fields if f in self.__parsed])

    # ### BEGIN collections.abc.MutableMapping methods

    def __iter__(self) -> Iterator[str]:
        for key in self.__keys:
            yield str(key)

    def __len__(self) -> int:
        return len(self.__keys)

    def __setitem__(self, key: str, value: Deb822ValueType) -> None:
        # The `_cached_strI` pays off in the long run (with Packages files or similar sized files)
        keyi = _cached_strI(key)
        self.__keys.add(keyi)
        self.__dict[keyi] = value

    def __getitem__(self, key: str) -> Deb822ValueType:
        keyi = _strI(key)
        try:
            value = self.__dict[keyi]
        except KeyError:
            if self.__parsed is not None and keyi in self.__keys:
                value = self.__parsed[keyi]
            else:
                raise

        # TODO(jsw): Move the decoding logic into __setitem__ so that we decode
        # it once instead of every time somebody asks for it.  Even better if
        # Deb822* classes dealt in pure unicode and didn't care about the
        # encoding of the files they came from...but I don't know how to fix
        # that without breaking a bunch of users.
        return self.decoder.decode(value)

    def __delitem__(self, key: str) -> None:
        keyi = _strI(key)
        self.__keys.remove(keyi)
        try:
            del self.__dict[keyi]
        except KeyError:
            # If we got this far, the key was in self.__keys, so it must have
            # only been in the self.__parsed dict.
            pass

    def __contains__(self, key: Any) -> bool:
        keyi = _strI(key)
        return keyi in self.__keys

    # ### END collections.abc.MutableMapping methods

    def order_last(self, field: str) -> None:
        """Re-order the given field so it is "last" in the paragraph"""
        self.__keys.order_last(_strI(field))

    def order_first(self, field: str) -> None:
        """Re-order the given field so it is "first" in the paragraph"""
        self.__keys.order_first(_strI(field))

    def order_before(self, field: str, reference_field: str) -> None:
        """Re-order the given field so appears directly after the reference field in the paragraph

        The reference field must be present."""
        self.__keys.order_before(_strI(field), _strI(reference_field))

    def order_after(self, field: str, reference_field: str) -> None:
        """Re-order the given field so appears directly before the reference field in the paragraph

        The reference field must be present.
        """
        self.__keys.order_after(_strI(field), _strI(reference_field))

    def sort_fields(self, key: Callable[[str], Any] | None = None) -> None:
        """Re-order all fields

        :param key: Provide a key function (same semantics as for sorted).  Keep in mind that
          Deb822 preserve the cases for field names - in generally, callers are recommended to use
          "lower()" to normalize the case.
        """
        if key is None:
            key = default_field_sort_key
        self.__keys = OrderedSet(sorted(self.__keys, key=key))

    def __repr__(self) -> str:
        return '{%s}' % ', '.join([f'{k!r}: {v!r}' for k, v in self.items()])

    def __eq__(self, other: Any) -> bool:
        mykeys = sorted(self)
        otherkeys = sorted(other)
        if not mykeys == otherkeys:
            return False

        for key in mykeys:
            if self[key] != other[key]:
                return False

        # If we got here, everything matched
        return True

    # Overriding __eq__ blocks inheritance of __hash__ in Python 3, and
    # instances of this class are not sensibly hashable anyway.
    __hash__ = None    # type: ignore

    def copy(self: T_Deb822Dict) -> T_Deb822Dict:
        # Use self.__class__ so this works as expected for subclasses
        copy = self.__class__(self)
        return copy

    # TODO implement __str__() and make dump() use that?


class Deb822(Deb822Dict):
    """ Generic Deb822 data

    :param sequence: a string, or any object that returns a line of
        input each time, normally a file.  Or a filename (as a str) or a
        `pathlib.Path` object. Alternately, sequence can
        be a dict that contains the initial key-value pairs. When
        python-apt is present, sequence can also be a compressed object,
        for example a file object associated to something.gz.

    :param fields: if given, it is interpreted as a list of fields that
        should be parsed (the rest will be discarded).

    :param _parsed: internal parameter.

    :param encoding: When parsing strings, interpret them in this encoding.
        (All values are given back as unicode objects, so an encoding is
        necessary in order to properly interpret the strings.)

    :param strict: Dict controlling the strictness of the internal parser
        to permit tuning of its behaviour between "generous in what it
        accepts" and "strict conformance". Known keys are described below.

    *Internal parser tuning*

    - `whitespace-separates-paragraphs`: (default: `True`)
      Blank lines between paragraphs should not have any whitespace in them
      at all. However:

      - Policy §5.1 permits `debian/control` in source packages to separate
        packages with lines containing whitespace to allow human edited
        files to have stray whitespace. Failing to honour this breaks
        tools such as
        `wrap-and-sort <https://manpages.debian.org/wrap-and-sort>`_
        (see, for example,
        `Debian Bug 715558 <https://bugs.debian.org/715558/>`_).
      - `apt_pkg.TagFile` accepts whitespace-only lines within the
        `Description` field; strictly matching the behaviour of apt's
        Deb822 parser requires setting this key to `False` (as is done
        by default for :class:`Sources` and :class:`Packages`.
        (see, for example,
        `Debian Bug 913274 <https://bugs.debian.org/913274/>`_).

    Note that these tuning parameter are only for the parser that is
    internal to `Deb822` and do not apply to python-apt's apt_pkg.TagFile
    parser which would normally be used for Packages and Sources files.
    """

    def __init__(self,
                 sequence: InputDataType | Deb822Mapping | Path | None = None,
                 fields: list[str] | None = None,
                 _parsed: Deb822 | TagSectionWrapper | None = None,
                 encoding: str = "utf-8",
                 strict: dict[str, bool] | None = None,
                 ) -> None:

        _dict: Mapping[str, str] = {}
        iterable: InputDataType | None = None
        if hasattr(sequence, 'items'):
            _dict = cast(Deb822Mapping, sequence)
        else:
            iterable = cast(InputDataType, sequence)

        Deb822Dict.__init__(self, _dict=_dict, _parsed=_parsed, _fields=fields,
                            encoding=encoding)

        if iterable is not None:
            try:
                self._internal_parser(iterable, fields, strict)
            except EOFError:
                pass

        self.gpg_info: GpgInfo | None = None
        #self.raw_text = None  # type: Optional[bytes]

    if TYPE_CHECKING:
        T_Deb822 = TypeVar('T_Deb822', bound='Deb822')

    @classmethod
    def iter_paragraphs(cls: type[T_Deb822],
                        sequence: InputDataType | Path,
                        fields: list[str] | None = None,
                        use_apt_pkg: bool = False,
                        shared_storage: bool = False,
                        encoding: str = "utf-8",
                        strict: dict[str, bool] | None = None,
                       ) -> Iterator[T_Deb822]:
        """Generator that yields a Deb822 object for each paragraph in sequence.

        :param sequence: same as in __init__.

        :param fields: likewise.

        :param use_apt_pkg: if sequence is a file, apt_pkg can be used
            if available to parse the file, since it's much much faster.  Set
            this parameter to True to enable use of apt_pkg. Note that the
            TagFile parser from apt_pkg is a much stricter parser of the
            Deb822 format, particularly with regards whitespace between
            paragraphs and comments within paragraphs. If these features are
            required (for example in debian/control files), ensure that this
            parameter is set to False.
        :param shared_storage: not used, here for historical reasons.  Deb822
            objects never use shared storage anymore.
        :param encoding: Interpret the paragraphs in this encoding.
            (All values are given back as unicode objects, so an encoding is
            necessary in order to properly interpret the strings.)
        :param strict: dict of settings to tune the internal parser if that is
            being used. See the documentation for :class:`Deb822` for details.
        """
        # pylint: disable=unused-argument

        is_filename_like = (
            isinstance(sequence, Path) or
            (isinstance(sequence, str) and sequence and "\n" not in sequence and
             Path(sequence).exists() and Path(sequence).is_file())
        )

        apt_pkg_allowed = use_apt_pkg and (
            _has_fileno(sequence) or is_filename_like
        )

        if use_apt_pkg and not _have_apt_pkg:
            # warn that apt_pkg was requested but not installed
            msg = (
                "Parsing of Deb822 data with python3-apt's apt_pkg was "
                "requested but this package is not importable. "
                "Is python3-apt installed?"
            )
            warnings.warn(msg)

        elif use_apt_pkg and not apt_pkg_allowed:
            # warn that apt_pkg was requested but can't be used
            msg = (
                "Parsing of Deb822 data with python3-apt's apt_pkg was "
                "requested but this cannot be done on non-file input."
            )
            warnings.warn(msg)

        if _have_apt_pkg and apt_pkg_allowed:
            # pylint: disable=no-member
            if isinstance(sequence, Path):
                sequence = str(sequence)
            parser = apt_pkg.TagFile(sequence, bytes=True)
            for section in parser:
                paragraph = cls(fields=fields,
                                _parsed=TagSectionWrapper(section, _AutoDecoder(encoding)),
                                encoding=encoding)
                if paragraph:
                    yield paragraph

        else:
            # Split this into multiple conditionals so that type checking
            # can follow the types through
            iterable: IterableInputDataType = []
            close_fh = False
            try:
                if is_filename_like:
                    # pylint: disable=consider-using-with
                    assert isinstance(sequence, (str, Path))
                    iterable = iter(open(sequence, encoding=encoding))
                    close_fh = True
                elif isinstance(sequence, str):
                    iterable = iter(sequence.splitlines())
                elif isinstance(sequence, bytes):  # repetition for mypy
                    iterable = iter(sequence.splitlines())
                else:
                    # StringIO/list can be iterated directly
                    iterable = iter(sequence)  # type: ignore
                while True:
                    x = cls(iterable, fields, encoding=encoding, strict=strict)
                    if not x:
                        break
                    yield x
            finally:
                if close_fh:
                    iterable.close()   # type: ignore

    ###

    @staticmethod
    def _skip_useless_lines(sequence: IterableInputDataType) -> Iterator[bytes]:
        """Yields only lines that do not begin with '#'.

        Also skips any blank lines at the beginning of the input.
        """
        at_beginning = True
        for line in sequence:
            # _skip_useless_lines is only called before one place and that prefers
            # bytes, so we can just convert the input into bytes and simplify
            # our checks.
            if isinstance(line, str):
                line = line.encode()
            if line.startswith(b'#'):
                continue
            if at_beginning:
                if not line.rstrip(b'\r\n'):
                    continue
                at_beginning = False
            yield line

    # regexps for parsing the Deb822 data
    # The key is non-whitespace, non-colon characters before any colon.
    _key_part = r"^(?P<key>[^: \t\n\r\f\v]+)\s*:\s*"
    _new_field_re = re.compile(_key_part + r"(?P<data>(?:\S+(\s+\S+)*)?)\s*$")

    # Explicit source entries in the file can be either:
    #   Source: source_package
    #   Source: source_package (1.2.3-1)
    _explicit_source_re = re.compile(
        r"(?P<source>[^ ]+)"
        r"( \((?P<version>.+)\))?"
    )

    def _internal_parser(self,
                         sequence: InputDataType,
                         fields: list[str] | None = None,
                         strict: dict[str, bool] | None = None,
                         ) -> None:

        def wanted_field(f: str) -> bool:
            return fields is None or f in fields

        if isinstance(sequence, (str, bytes)):
            sequence = sequence.splitlines()

        curkey = None
        content = ""

        for linebytes in self._gpg_stripped_paragraph(
                self._skip_useless_lines(sequence), strict):
            line = self.decoder.decode_bytes(linebytes)

            m = self._new_field_re.match(line)
            if m:
                if curkey:
                    self[curkey] = content

                curkey = m.group('key')

                if not wanted_field(curkey):
                    curkey = None
                    continue

                content = m.group('data')
                continue

            # Skip lines that entirely whitespace
            if line and line[0].isspace() and not line.isspace():
                content += '\n' + line
                continue

        if curkey:
            self[curkey] = content

    def __str__(self) -> str:
        d = self.dump()
        return d if d is not None else ""

    def __bytes__(self) -> bytes:
        d = self.dump()
        return d.encode(self.encoding) if d is not None else b""

    # __repr__ is handled by Deb822Dict

    def get_as_string(self, key: str) -> str:
        """Return the self[key] as a string (or unicode)

        The default implementation just returns unicode(self[key]); however,
        this can be overridden in subclasses (e.g. _multivalued) that can take
        special values.
        """
        return str(self[key])

    def _dump_format(self) -> Generator[str]:
        for key in self:
            value = self.get_as_string(key)
            if not value or value[0] == '\n':
                # Avoid trailing whitespace after "Field:" if it's on its own
                # line or the value is empty.  We don't have to worry about the
                # case where value == '\n', since we ensure that is not the
                # case in __setitem__.
                entry = f'{key}:{value}\n'
            else:
                entry = f'{key}: {value}\n'
            yield entry

    def _dump_str(self) -> str:
        return "".join(self._dump_format())

    def _dump_fd_b(self,
                   fd: IO[bytes],
                   encoding: str,
                   ) -> None:
        for entry in self._dump_format():
            fd.write(entry.encode(encoding))

    def _dump_fd_t(self,
                   fd: IO[str],
                   ) -> None:
        for entry in self._dump_format():
            fd.write(entry)

    @overload
    def dump(self) -> str:
        pass

    @overload
    def dump(self,
             fd: IO[bytes],
             encoding: str | None = None,
             text_mode: Literal[False] = False,
            ) -> None:
        pass

    @overload
    def dump(self,
             fd: IO[str],
             encoding: str | None = None,
             text_mode: Literal[True] = True,
            ) -> None:
        pass

    @overload
    def dump(self,
             fd: Literal[None] = None,
             encoding: Literal[None] = None,
             text_mode: bool = False,
             ) -> str:
        pass

    @overload
    def dump(self,
             fd: IO[str] | IO[bytes] | None = None,
             encoding: str | None = None,
             text_mode: bool = False,
            ) -> str | None:
        pass

    def dump(self,
             fd: IO[str] | IO[bytes] | None = None,
             encoding: str | None = None,
             text_mode: bool = False,
            ) -> str | None:
        """Dump the contents in the original format

        :param fd: file-like object to which the data should be written
            (see notes below)
        :param encoding: str, optional (Defaults to object default).
            Encoding to use when writing out the data.
        :param text_mode: bool, optional (Defaults to ``False``).
            Encoding should be undertaken by this function rather than by the
            caller.

        If fd is None, returns a unicode object.  Otherwise, fd is assumed to
        be a file-like object, and this method will write the data to it
        instead of returning a unicode object.

        If fd is not none and text_mode is False, the data will be encoded
        to a byte string before writing to the file.  The encoding used is
        chosen via the encoding parameter; None means to use the encoding the
        object was initialized with (utf-8 by default).  This will raise
        UnicodeEncodeError if the encoding can't support all the characters in
        the Deb822Dict values.
        """
        # Ideally this would never try to encode (that should be up to the
        # caller when opening the file), but we may still have users who rely
        # on the binary mode encoding.  But...might it be better to break them
        # than to introduce yet another parameter relating to encoding?

        if fd is None:
            return self._dump_str()

        if text_mode:
            self._dump_fd_t(cast(IO[str], fd))
        else:
            if encoding is None:
                # Use the encoding we've been using to decode strings with if none
                # was explicitly specified
                encoding = self.encoding

            self._dump_fd_b(cast(IO[bytes], fd), encoding)
        return None

    ###

    @staticmethod
    def is_single_line(s: str) -> bool:
        return not s.count("\n")

    @staticmethod
    def is_multi_line(s: str) -> bool:
        return not Deb822.is_single_line(s)

    def _merge_fields(self,
                      s1: str,
                      s2: str,
                      ) -> str:
        if not s2:
            return s1
        if not s1:
            return s2

        if self.is_single_line(s1) and self.is_single_line(s2):
            # some fields are delimited by a single space, others
            # a comma followed by a space.  this heuristic assumes
            # that there are multiple items in one of the string fields
            # so that we can pick up on the delimiter being used
            delim = ' '
            if (s1 + s2).count(', '):
                delim = ', '

            L = sorted((s1 + delim + s2).split(delim))

            prev = merged = L[0]

            for item in L[1:]:
                # skip duplicate entries
                if item == prev:
                    continue
                merged = merged + delim + item
                prev = item
            return merged

        if self.is_multi_line(s1) and self.is_multi_line(s2):
            for item in s2.splitlines():
                if item not in s1.splitlines():
                    s1 = s1 + "\n" + item
            return s1

        raise ValueError

    def merge_fields(self,
                     key: str,
                     d1: Mapping[str, str],
                     d2: Mapping[str, str] | None = None,
                     ) -> str | None:
        # this method can work in two ways - abstract that away
        if d2 is None:
            x1: Mapping[str, str] | Deb822 = self
            x2 = d1
        else:
            x1 = d1
            x2 = d2

        # we only have to do work if both objects contain our key
        # otherwise, we just take the one that does, or raise an
        # exception if neither does
        if key in x1 and key in x2:
            merged = self._merge_fields(x1[key], x2[key])
        elif key in x1:
            merged = x1[key]
        elif key in x2:
            merged = x2[key]
        else:
            raise KeyError

        # back to the two different ways - if this method was called
        # upon an object, update that object in place.
        # return nothing in this case, to make the author notice a
        # problem if she assumes the object itself will not be modified
        if d2 is None:
            self[key] = merged
            return None

        return merged

    # regexps for finding the gpg header around signed data
    _gpgre = re.compile(br'^-----(?P<action>BEGIN|END) '
                        br'PGP (?P<what>[^-]+)-----[\r\t ]*$')

    @staticmethod
    def split_gpg_and_payload(sequence: Iterator[bytes] | Iterator[str],
                              strict: dict[str, bool] | None = None,
                              ) -> tuple[list[bytes], list[bytes], list[bytes]]:
        """Return a (gpg_pre, payload, gpg_post) tuple

        Each element of the returned tuple is a list of lines (with trailing
        whitespace stripped).

        :param sequence: iterable.
            An iterable that yields lines of data (str, unicode,
            bytes) to be parsed, possibly including a GPG in-line signature.
        :param strict: dict, optional.
            Control over the strictness of the parser. See the :class:`Deb822`
            class documentation for details.
        """
        # Some consumers of this method require bytes (encoding
        # detection and signature checking).  However, we might have
        # been given a file opened in text mode, in which case it's
        # simplest to encode to bytes.
        _encoded_sequence = (x.encode() if isinstance(x, str) else x for x in sequence)
        return Deb822._split_gpg_and_payload(_encoded_sequence, strict=strict)

    @staticmethod
    def _split_gpg_and_payload(sequence: Iterator[bytes],
                               strict: dict[str, bool] | None = None,
                               ) -> tuple[list[bytes], list[bytes], list[bytes]]:
        # pylint: disable=too-many-branches

        if not strict:
            strict = {}

        gpg_pre_lines: list[bytes] = []
        lines: list[bytes] = []
        gpg_post_lines: list[bytes] = []
        state = b'SAFE'

        # Include whitespace-only lines in blank lines to split paragraphs.
        # (see #715558)
        accept_empty_or_whitespace = strict.get('whitespace-separates-paragraphs', True)
        first_line = True

        for line in sequence:
            line = line.strip(b'\r\n')

            # skip initial blank lines, if any
            if first_line:
                if not line or line.isspace():
                    continue
                first_line = False

            m = Deb822._gpgre.match(line) if line.startswith(b'-') else None
            # We unconditionally compute whether it is a blank line.  We need it for all lines
            # that are not GPG lines (which is the vast major of lines).  For Packages files,
            # using this "simple" solution is about 5% faster than regexes.
            is_empty_line = not line or line.isspace() if accept_empty_or_whitespace else not line

            if not m:
                if state == b'SAFE':
                    if not is_empty_line:
                        lines.append(line)
                    else:
                        if not gpg_pre_lines:
                            # There's no gpg signature, so we should stop at
                            # this blank line
                            break
                elif state == b'SIGNED MESSAGE':
                    if is_empty_line:
                        state = b'SAFE'
                    else:
                        gpg_pre_lines.append(line)
                elif state == b'SIGNATURE':
                    gpg_post_lines.append(line)
            else:
                if m.group('action') == b'BEGIN':
                    state = m.group('what')
                elif m.group('action') == b'END':
                    gpg_post_lines.append(line)
                    break
                if not is_empty_line:
                    if not lines:
                        gpg_pre_lines.append(line)
                    else:
                        gpg_post_lines.append(line)

        if lines:
            return (gpg_pre_lines, lines, gpg_post_lines)

        raise EOFError('only blank lines found in input')

    @classmethod
    def _gpg_stripped_paragraph(cls,
                                sequence: Iterator[bytes],
                                strict: dict[str, bool] | None = None) -> list[bytes]:
        return cls._split_gpg_and_payload(sequence, strict)[1]

    def get_gpg_info(self, keyrings: Iterable[str] | None = None) -> GpgInfo:
        """Return a GpgInfo object with GPG signature information

        This method will raise ValueError if the signature is not available
        (e.g. the original text cannot be found).

        :param keyrings: list of keyrings to use (see GpgInfo.from_sequence)
        """

        # raw_text is saved (as a string) only for Changes and Dsc (see
        # _gpg_multivalued.__init__) which is small compared to Packages or
        # Sources which contain no signature
        if not hasattr(self, 'raw_text'):
            raise ValueError("original text cannot be found")

        if self.gpg_info is None:
            # pylint: disable=no-member
            # (raw_text is checked above)
            self.gpg_info = GpgInfo.from_sequence(self.raw_text,
                                                  keyrings=keyrings)

        return self.gpg_info

    def validate_input(self, key: str, value: str) -> None:
        # pylint: disable=unused-argument
        """Raise ValueError if value is not a valid value for key

        Subclasses that do interesting things for different keys may wish to
        override this method.
        """
        # FIXME: key is not validated, contrary to docstring

        if '\n' not in value:
            return

        # The value cannot end in a newline (if it did, dumping the object
        # would result in multiple stanzas)
        if value.endswith('\n'):
            raise ValueError("value must not end in '\\n'")

        # Make sure there are no blank lines (actually, the first one is
        # allowed to be blank, but no others), and each subsequent line starts
        # with whitespace
        for no, line in enumerate(value.splitlines()):
            if no == 0:
                continue
            if not line:
                raise ValueError("value must not have blank lines")
            if not line[0].isspace():
                raise ValueError("each line must start with whitespace")

    def __setitem__(self, key: str, value: str) -> None:
        self.validate_input(key, value)
        Deb822Dict.__setitem__(self, key, value)


if TYPE_CHECKING:
    _BaseGpgInfo = Dict[str, List[str]]
else:
    _BaseGpgInfo = dict


# XXX check what happens if input contains more that one signature
class GpgInfo(_BaseGpgInfo):
    """A wrapper around gnupg parsable output obtained via --status-fd

    This class is really a dictionary containing parsed output from gnupg plus
    some methods to make sense of the data.
    Keys are keywords and values are arguments suitably split.
    See /usr/share/doc/gnupg/DETAILS.gz"""

    # keys with format "key keyid uid"
    uidkeys = ('GOODSIG', 'EXPSIG', 'EXPKEYSIG', 'REVKEYSIG', 'BADSIG')

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.out: list[str] | None = None
        self.err: list[str] | None = None

    def valid(self) -> bool:
        """Is the signature valid?"""
        return 'GOODSIG' in self or 'VALIDSIG' in self

# XXX implement as a property?
# XXX handle utf-8 %-encoding
    def uid(self) -> None:
        """Return the primary ID of the signee key, None is not available"""

    @classmethod
    def from_output(cls,
                    out: str | list[str],
                    err: str | list[str] | None = None) -> GpgInfo:
        """ Create a GpgInfo object based on the gpg or gpgv output

        Create a new GpgInfo object from gpg(v) --status-fd output (out) and
        optionally collect stderr as well (err).

        Both out and err can be lines in newline-terminated sequence or
        regular strings.
        """

        n = cls()

        if isinstance(out, str):
            n.out = out.split('\n')
        else:
            n.out = out

        if isinstance(err, str):
            n.err = err.split('\n')
        else:
            n.err = err

        header = '[GNUPG:] '
        for line in n.out:
            if not line.startswith(header):
                continue

            line = line[len(header):]
            line = line.strip('\n')

            # str.partition() would be better, 2.5 only though
            s = line.find(' ')
            key = line[:s]
            if key in cls.uidkeys:
                # value is "keyid UID", don't split UID
                value = line[s+1:].split(' ', 1)
            else:
                value = line[s+1:].split(' ')

            # Skip headers in the gpgv output that are not interesting
            # note NEWSI is actually NEWSIG but the above parsing loses the 'G'
            # if no keyid is included in the message. See
            # /usr/share/doc/gnupg/DETAILS.gz
            if key in ('NEWSI', 'NEWSIG', 'KEY_CONSIDERED', 'PROGRESS'):
                continue

            n[key] = value
        return n

    @classmethod
    def from_sequence(cls,
                      sequence: bytes | Iterable[bytes],
                      keyrings: Iterable[str] | None = None,
                      executable: Iterable[str] | None = None
                     ) -> GpgInfo:
        """Create a new GpgInfo object from the given sequence.

        :param sequence: sequence of lines of bytes or a single byte string

        :param keyrings: list of keyrings to use (default:
            ['/usr/share/keyrings/debian-keyring.gpg'])

        :param executable: list of args for subprocess.Popen, the first element
            being the gpgv executable (default: ['/usr/bin/gpgv'])
        """

        keyrings = keyrings or GPGV_DEFAULT_KEYRINGS
        executable = executable or [GPGV_EXECUTABLE]

        # XXX check for gpg as well and use --verify accordingly?
        args = list(executable)
        # args.extend(["--status-fd", "1", "--no-default-keyring"])
        args.extend(["--status-fd", "1"])
        for k in keyrings:
            args.extend(["--keyring", k])

        if "--keyring" not in args:
            raise OSError("cannot access any of the given keyrings")

        with subprocess.Popen(
            args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=False,
        ) as p:
            # XXX what to do with exit code?

            if isinstance(sequence, bytes):
                inp = sequence
            else:
                inp = cls._get_full_bytes(sequence)
            out, err = p.communicate(inp)

        return cls.from_output(out.decode('unicode-escape'),
                               err.decode('unicode-escape'))

    @staticmethod
    def _get_full_bytes(sequence: Iterable[bytes]) -> bytes:
        """Return a byte string from a sequence of lines of bytes.

        This method detects if the sequence's lines are newline-terminated, and
        constructs the byte string appropriately.
        """
        # Peek at the first line to see if it's newline-terminated.
        sequence_iter = iter(sequence)
        try:
            first_line = next(sequence_iter)
        except StopIteration:
            return b""
        join_str = b'\n'
        if first_line.endswith(b'\n'):
            join_str = b''
        return first_line + join_str + join_str.join(sequence_iter)

    @classmethod
    def from_file(cls, target: str, *args: Any, **kwargs: Any) -> GpgInfo:
        """Create a new GpgInfo object from the given file.

        See GpgInfo.from_sequence.
        """
        with open(target, 'rb') as target_file:
            return cls.from_sequence(target_file, *args, **kwargs)


class PkgRelation:
    """Inter-package relationships

    Structured representation of the relationships of a package to another,
    i.e. of what can appear in a Deb882 field like Depends, Recommends,
    Suggests, ... (see Debian Policy 7.1).
    """

    # XXX *NOT* a real dependency parser, and that is not even a goal here, we
    # just parse as much as we need to split the various parts composing a
    # dependency, checking their correctness wrt policy is out of scope
    __dep_RE = re.compile(
        r'^\s*(?P<name>[a-zA-Z0-9][a-zA-Z0-9.+\-]*)'
        r'(:(?P<archqual>([a-zA-Z0-9][a-zA-Z0-9-]*)))?'
        r'(\s*\(\s*(?P<relop>[>=<]+)\s*'
        r'(?P<version>[0-9a-zA-Z:\-+~.]+)\s*\))?'
        r'(\s*\[(?P<archs>[\s!\w\-]+)\])?\s*'
        r'((?P<restrictions><.+>))?\s*'
        r'$')
    __comma_sep_RE = re.compile(r'\s*,\s*')
    __pipe_sep_RE = re.compile(r'\s*\|\s*')
    __blank_sep_RE = re.compile(r'\s+')
    __restriction_sep_RE = re.compile(r'>\s*<')
    __restriction_RE = re.compile(
        r'(?P<enabled>\!)?'
        r'(?P<profile>[^\s]+)')

    class ArchRestriction(NamedTuple):
        enabled: bool
        arch: str

    class BuildRestriction(NamedTuple):
        enabled: bool
        profile: str

    if TYPE_CHECKING:
        class ParsedRelation(TypedDict):
            name: str
            archqual: str | None
            version: tuple[str, str] | None
            arch: list[PkgRelation.ArchRestriction] | None
            restrictions: list[list[PkgRelation.BuildRestriction]] | None

    @classmethod
    def parse_relations(cls, raw: str) -> list[list[PkgRelation.ParsedRelation]]:
        """Parse a package relationship string (i.e. the value of a field like
        Depends, Recommends, Build-Depends ...)
        """
        def parse_archs(raw: str) -> list[PkgRelation.ArchRestriction]:
            # assumption: no space between '!' and architecture name
            archs = []
            for arch in cls.__blank_sep_RE.split(raw.strip()):
                disabled = arch[0] == '!'
                if disabled:
                    arch = arch[1:]
                archs.append(cls.ArchRestriction(not disabled, arch))
            return archs

        def parse_restrictions(raw: str) -> list[list[PkgRelation.BuildRestriction]]:
            """ split a restriction formula into a list of restriction lists

            Each term in the restriction list is a namedtuple of form:

                (enabled, label)

            where
                enabled: bool: whether the restriction is positive or negative
                profile: the profile name of the term e.g. 'stage1'
            """
            restrictions = []
            groups = cls.__restriction_sep_RE.split(raw.lower().strip('<> '))
            for rgrp in groups:
                group = []
                for restriction in cls.__blank_sep_RE.split(rgrp):
                    match = cls.__restriction_RE.match(restriction)
                    if match:
                        parts = match.groupdict()
                        group.append(
                            cls.BuildRestriction(
                                parts['enabled'] != '!',
                                parts['profile'],
                            ))
                restrictions.append(group)
            return restrictions

        def parse_rel(raw: str) -> PkgRelation.ParsedRelation:
            match = cls.__dep_RE.match(raw)
            if match:
                parts = match.groupdict()
                d: PkgRelation.ParsedRelation = {
                    'name': parts['name'],
                    'archqual': parts['archqual'],
                    'version': None,
                    'arch': None,
                    'restrictions': None,
                }
                if parts['relop'] or parts['version']:
                    d['version'] = (parts['relop'], parts['version'])
                if parts['archs']:
                    d['arch'] = parse_archs(parts['archs'])
                if parts['restrictions']:
                    d['restrictions'] = parse_restrictions(
                        parts['restrictions'])
                return d

            logger.warning(
                'cannot parse package'
                ' relationship "%s", returning it raw', raw)
            return {
                'name': raw,
                'archqual': None,
                'version': None,
                'arch': None,
                'restrictions': None,
            }

        tl_deps = cls.__comma_sep_RE.split(raw.strip())   # top-level deps
        cnf = map(cls.__pipe_sep_RE.split, tl_deps)
        return [[parse_rel(or_dep) for or_dep in or_deps] for or_deps in cnf]

    @staticmethod
    def holds_on_arch(
        relation: ParsedRelation,
        arch: str,
        table: debian.debian_support.DpkgArchTable,
    ) -> bool:
        # NOTE! The DpkgArchTable is stubbed in including doctests to support non-Debian systems
        #   See conftest.py for the concrete implementation and the limited data set available.
        """Is relation active on the given architecture?

        >>> table = DpkgArchTable.load_arch_table()
        >>> relation = PkgRelation.parse_relations("foo [armel linux-any],")[0][0]
        >>> PkgRelation.holds_on_arch(relation, "amd64", table)
        True
        >>> PkgRelation.holds_on_arch(relation, "hurd-i386", table)
        False
        """
        archs = relation["arch"]
        return (archs is None
                or table.architecture_is_concerned(
                    arch,
                    tuple(("" if a.enabled else "!") + a.arch for a in archs)))

    @staticmethod
    def holds_with_profiles(
        relation: ParsedRelation,
        profiles: collections.abc.Container[str],
    ) -> bool:
        """Is relation active under the given profiles?

        >>> relation = PkgRelation.parse_relations("foo <a !b> <c>")[0][0]
        >>> PkgRelation.holds_with_profiles(relation, ("a", "b"))
        False
        >>> PkgRelation.holds_with_profiles(relation, ("c", ))
        True
        """
        restrictions = relation["restrictions"]
        return (restrictions is None
                or any(all(term.enabled == (term.profile in profiles)
                           for term in restriction_list)
                       for restriction_list in restrictions))

    @staticmethod
    def str(rels: list[list[PkgRelation.ParsedRelation]]) -> builtins.str:
        """Format to string structured inter-package relationships

        Perform the inverse operation of parse_relations, returning a string
        suitable to be written in a package stanza.
        """
        def pp_arch(arch_spec: PkgRelation.ArchRestriction) -> str:
            return '{}{}'.format(
                '' if arch_spec.enabled else '!',
                arch_spec.arch,
            )

        def pp_restrictions(restrictions: list[PkgRelation.BuildRestriction]) -> str:
            s = []
            for term in restrictions:
                s.append(
                    '{}{}'.format(
                        '' if term.enabled else '!',
                        term.profile
                    )
                )
            return '<%s>' % ' '.join(s)

        def pp_atomic_dep(dep: PkgRelation.ParsedRelation) -> str:
            s: str = dep['name']
            if dep.get('archqual') is not None:
                s += ':%s' % dep['archqual']

            v = dep.get('version')
            if v is not None:
                s += ' (%s %s)' % v

            a = dep.get('arch')
            if a is not None:
                s += ' [%s]' % ' '.join(map(pp_arch, a))

            r = dep.get('restrictions')
            if r is not None:
                s += ' %s' % ' '.join(map(pp_restrictions, r))
            return s

        return ', '.join(
            map(lambda deps: ' | '.join(map(pp_atomic_dep, deps)), rels))


if TYPE_CHECKING:
    _lowercase_dict_base = Dict[str, Any]
else:
    _lowercase_dict_base = dict


class _lowercase_dict(_lowercase_dict_base):
    """Dictionary wrapper which lowercase keys upon lookup."""

    def __getitem__(self, key: str) -> Any | None:
        return dict.__getitem__(self, key.lower())


if TYPE_CHECKING:
    class _HasVersionFieldProtocol(Protocol):

        def __getitem__(self, s: str) -> str:
            pass

        def __setitem__(self, s: str, v: str) -> None:
            pass


class _VersionAccessorMixin:
    """Give access to Version keys as debian_support.Version objects."""
    def get_version(self: _HasVersionFieldProtocol) -> debian.debian_support.Version:
        return debian.debian_support.Version(self['Version'])

    def set_version(self: _HasVersionFieldProtocol,
                    version: debian.debian_support.Version) -> None:
        self['Version'] = str(version)



class _PkgRelationMixin:
    """Package relationship mixin

    Inheriting from this mixin you can extend a :class:`Deb822` object with
    attributes letting you access inter-package relationship in a structured
    way, rather than as strings.
    For example, while you can usually use ``pkg['depends']`` to
    obtain the Depends string of package pkg, mixing in with this class you
    gain pkg.depends to access Depends as a Pkgrel instance

    To use, subclass _PkgRelationMixin from a class with a _relationship_fields
    attribute. It should be a list of field names for which structured access
    is desired; for each of them a method wild be added to the inherited class.
    The method name will be the lowercase version of field name; '-' will be
    mangled as '_'. The method would return relationships in the same format of
    the PkgRelation' relations property.

    See Packages and Sources as examples.
    """
    _relationship_fields: list[str] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # pylint: disable=unused-argument
        # (accept anything via constructors)

        self.__relations = _lowercase_dict({})
        self.__parsed_relations = False
        for name in self._relationship_fields:
            # To avoid reimplementing Deb822 key lookup logic we use a really
            # simple dict subclass which just lowercase keys upon lookup. Since
            # dictionary building happens only here, we ensure that all keys
            # are in fact lowercase.
            # With this trick we enable users to use the same key (i.e. field
            # name) of Deb822 objects on the dictionary returned by the
            # relations property.
            keyname = name.lower()
            if name in self:    # type: ignore # Mixin is used with Deb822Dict
                self.__relations[keyname] = None   # lazy value
                # all lazy values will be expanded before setting
                # __parsed_relations to True
            else:
                self.__relations[keyname] = []

    @property
    def relations(self) -> _lowercase_dict:
        """Return a dictionary of inter-package relationships among the current
        and other packages.

        Dictionary keys depend on the package kind. Binary packages have keys
        like 'depends', 'recommends', ... while source packages have keys like
        'build-depends', 'build-depends-indep' and so on. See the Debian policy
        for the comprehensive field list.

        Dictionary values are package relationships returned as lists of lists
        of dictionaries (see below for some examples).

        The encoding of package relationships is as follows:

        - the top-level lists corresponds to the comma-separated list of
          :class:`Deb822`, their components form a conjunction, i.e. they
          have to be AND-ed together
        - the inner lists corresponds to the pipe-separated list of
          :class:`Deb822`,
          their components form a disjunction, i.e. they have to be OR-ed
          together
        - member of the inner lists are dictionaries with the following keys:

          ``name``
            package (or virtual package) name
          ``version``
            A pair <`operator`, `version`> if the relationship is
            versioned, None otherwise. operator is one of ``<<``,
            ``<=``, ``=``, ``>=``, ``>>``; version is the given version as
            a string.
          ``arch``
            A list of pairs <`enabled`, `arch`> if the
            relationship is architecture specific, None otherwise.
            Enabled is a boolean (``False`` if the architecture is
            negated with ``!``, ``True`` otherwise), arch the
            Debian architecture name as a string.
          ``restrictions``
            A list of lists of tuples <`enabled`, `profile`>
            if there is a restriction formula defined, ``None``
            otherwise. Each list of tuples represents a restriction
            list while each tuple represents an individual term
            within the restriction list. Enabled is a boolean
            (``False`` if the restriction is negated with ``!``,
            ``True`` otherwise). The profile is the name of the
            build restriction.
            https://wiki.debian.org/BuildProfileSpec

          The arch and restrictions tuples are available as named tuples so
          elements are available as `term[0]` or alternatively as
          `term.enabled` (and so forth).

        Examples:

        ``"emacs | emacsen, make, debianutils (>= 1.7)"``
        becomes::

          [
            [ {'name': 'emacs'}, {'name': 'emacsen'} ],
            [ {'name': 'make'} ],
            [ {'name': 'debianutils', 'version': ('>=', '1.7')} ]
          ]

        ``"tcl8.4-dev, procps [!hurd-i386]"``
        becomes::

          [
            [ {'name': 'tcl8.4-dev'} ],
            [ {'name': 'procps', 'arch': (false, 'hurd-i386')} ]
          ]

        ``"texlive <!cross>"``
        becomes::

          [
            [ {'name': 'texlive', 'restriction': [[(false, 'cross')]]} ]
          ]
        """
        if not self.__parsed_relations:
            lazy_rels = filter(lambda n: self.__relations[n] is None,
                               self.__relations.keys())
            for n in lazy_rels:
                # Mixin is used with Deb822Dict so self becomes indexable
                self.__relations[n] = PkgRelation.parse_relations(self[n]) # type: ignore
            self.__parsed_relations = True
        return self.__relations


class _multivalued(Deb822):
    """A class with (R/W) support for multivalued fields.

    To use, create a subclass with a _multivalued_fields attribute.  It should
    be a dictionary with *lower-case* keys, with lists of human-readable
    identifiers of the fields as the values.
    Please see :class:`Dsc`, :class:`Changes`, and :class:`PdiffIndex`
    as examples.
    """
    _multivalued_fields: dict[str, list[str]] = {}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        Deb822.__init__(self, *args, **kwargs)

        for field, fields in self._multivalued_fields.items():
            try:
                contents = self[field]
            except KeyError:
                continue

            if self.is_multi_line(contents):
                self[field] = []    # type: ignore
                updater_method = self[field].append
            else:
                self[field] = Deb822Dict()    # type: ignore
                updater_method = self[field].update

            for line in filter(None, contents.splitlines()):   # type: str
                updater_method(Deb822Dict(zip(fields, line.split(maxsplit=len(fields) - 1))))

    def validate_input(self, key: str, value: list[dict[str, str]] | str) -> None:
        if key.lower() in self._multivalued_fields:
            # It's difficult to write a validator for multivalued fields, and
            # basically futile, since we allow mutable lists.  In any case,
            # with sanity checking in get_as_string, we shouldn't ever output
            # unparseable data.
            pass
        else:
            super().validate_input(key, value)  # type: ignore

    def get_as_string(self, key: str) -> str:
        keyl = key.lower()
        if keyl in self._multivalued_fields:
            fd = io.StringIO()
            if hasattr(self[key], 'keys'):   # single-line
                array = [self[key]]
            else:   # multi-line
                fd.write("\n")
                array = self[key]

            order = self._multivalued_fields[keyl]
            field_lengths: Mapping[str, Mapping[str, int]] = {}
            try:
                field_lengths = self._fixed_field_lengths  # type: ignore  # lazy added member
            except AttributeError:
                pass
            for item in array:
                for x in order:
                    try:
                        raw_value = str(item[x])
                    except KeyError:
                        break
                    try:
                        length = field_lengths[keyl][x]
                    except KeyError:
                        value = raw_value
                    else:
                        value = (length - len(raw_value)) * " " + raw_value
                    if "\n" in value:
                        raise ValueError("'\\n' not allowed in component of "
                                         "multivalued field %s" % key)
                    fd.write(" %s" % value)
                fd.write("\n")
            return fd.getvalue().rstrip("\n")

        return Deb822.get_as_string(self, key)


class _gpg_multivalued(_multivalued):
    """A _multivalued class that can support gpg signed objects

    This class's feature is that it stores the raw text before parsing so that
    gpg can verify the signature.  Use it just like you would use the
    _multivalued class.

    This class only stores raw text if it is given a raw string, or if it
    detects a gpg signature when given a file or sequence of lines (see
    Deb822.split_gpg_and_payload for details).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.raw_text: bytes | None = None
        try:
            sequence = args[0]
        except IndexError:
            sequence = kwargs.get("sequence", None)
        strict = kwargs.get("strict", None)

        if sequence is not None:
            # If the input is a unicode object or a file opened in text mode,
            # we'll need to encode it back to bytes for gpg.  If it's not
            # actually in the encoding that we guess, then this probably won't
            # verify correctly, but this is the best we can reasonably manage.
            # For accurate verification, the file should be opened in binary
            # mode.
            encoding = (getattr(sequence, 'encoding', None)
                        or kwargs.get('encoding', 'utf-8') or 'utf-8')
            if isinstance(sequence, bytes):
                self.raw_text = sequence
            elif isinstance(sequence, str):
                self.raw_text = sequence.encode(encoding)
            elif hasattr(sequence, "items"):
                # sequence is actually a dict(-like) object, so we don't have
                # the raw text.
                pass
            else:
                try:
                    gpg_pre_lines, lines, gpg_post_lines = \
                        self.split_gpg_and_payload(
                            (self._bytes(s, encoding) for s in sequence),
                            strict)
                except EOFError:
                    # Empty input
                    gpg_pre_lines = lines = gpg_post_lines = []
                if gpg_pre_lines and gpg_post_lines:
                    raw_text = io.BytesIO()
                    raw_text.write(b"\n".join(gpg_pre_lines))
                    raw_text.write(b"\n\n")
                    raw_text.write(b"\n".join(lines))
                    raw_text.write(b"\n\n")
                    raw_text.write(b"\n".join(gpg_post_lines))
                    self.raw_text = raw_text.getvalue()
                try:
                    argsl = list(args)
                    argsl[0] = lines
                    args = tuple(argsl)
                except IndexError:
                    kwargs["sequence"] = lines

        _multivalued.__init__(self, *args, **kwargs)

    @staticmethod
    def _bytes(s: bytes | str, encoding: str) -> bytes:
        """Converts s to bytes if necessary, using encoding.

        If s is already bytes type, returns it directly.
        """
        if isinstance(s, bytes):
            return s
        if isinstance(s, str):
            return s.encode(encoding)
        raise TypeError('bytes or unicode/string required, not %s' % type(s))


class Dsc(_gpg_multivalued, _VersionAccessorMixin):
    """ Representation of a .dsc (Debian Source Control) file

    This class is a thin wrapper around the transparent GPG handling
    of :class:`_gpg_multivalued` and the parsing of :class:`Deb822`.
    """
    _multivalued_fields = {
        "files": ["md5sum", "size", "name"],
        "checksums-sha1": ["sha1", "size", "name"],
        "checksums-sha256": ["sha256", "size", "name"],
        "checksums-sha512": ["sha512", "size", "name"],
        "package-list": ["package", "package-type", "section", "priority", "_other"],
    }


class Changes(_gpg_multivalued, _VersionAccessorMixin):
    """ Representation of a .changes (archive changes) file

    This class is a thin wrapper around the transparent GPG handling
    of :class:`_gpg_multivalued` and the parsing of :class:`Deb822`.
    """
    _multivalued_fields = {
        "files": ["md5sum", "size", "section", "priority", "name"],
        "checksums-sha1": ["sha1", "size", "name"],
        "checksums-sha256": ["sha256", "size", "name"],
        "checksums-sha512": ["sha512", "size", "name"],
        "package-list": ["package", "package-type", "section", "priority", "key-value-list"],
    }

    def get_pool_path(self) -> str:
        """Return the path in the pool where the files would be installed"""

        # This is based on the section listed for the first file.  While
        # it is possible, I think, for a package to provide files in multiple
        # sections, I haven't seen it in practice.  In any case, this should
        # probably detect such a situation and complain, or return a list...

        s = self['files'][0]['section']

        try:
            section, _ = s.split('/')
        except ValueError:
            # main is implicit
            section = 'main'

        if self['source'].startswith('lib'):
            subdir = self['source'][:4]
        else:
            subdir = self['source'][0]

        return 'pool/{}/{}/{}'.format(section, subdir, self['source'])

    def _merge_check_simple_fields(self, other: Changes) -> None:
        """Check whether simple fields in .changes files are consistent."""
        if self["Format"] != "1.8":
            raise MergeChangesError(
                f"Unknown .changes format: {self['Format']}"
            )

        for field in ("Format", "Source", "Version"):
            values = [changes[field] for changes in (self, other)]
            if len(set(values)) != 1:
                raise MergeChangesError(
                    f"{field} fields do not match: {values}"
                )

    def _merge_check_descriptions(self, other: Changes) -> None:
        """Check that descriptions in .changes files are consistent."""
        changes_description_re = re.compile(r"^ ([^ ]+) - (.+)")
        descriptions: dict[str, str] = {}
        for changes in (self, other):
            for description_line in changes.get(
                "Description", ""
            ).splitlines():
                if not description_line:
                    continue
                m = changes_description_re.match(description_line)
                if m:
                    name, description = m.groups()
                    if (
                        name in descriptions
                        and descriptions[name] != description
                    ):
                        raise MergeChangesError(
                            f"Descriptions for {name} do not match: "
                            f"{descriptions[name]!r} != {description!r}"
                        )
                    descriptions[name] = description

    def _merge_check_checksums(self, other: Changes) -> None:
        """Check that checksums in .changes files are consistent."""
        checksums: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
        for changes in (self, other):
            for field in changes:
                if field.lower().startswith(
                    "checksums-"
                ) and field.lower() not in {
                    "checksums-sha1",
                    "checksums-sha256",
                }:
                    raise MergeChangesError(
                        f"Unsupported checksum field: {field}"
                    )

            for field in ("Files", "Checksums-Sha1", "Checksums-Sha256"):
                for checksum in changes.get(field, []):
                    name = checksum["name"]
                    if (
                        name in checksums[field]
                        and checksums[field][name] != checksum
                    ):
                        raise MergeChangesError(
                            f"Entries in {field} for {name} do not match: "
                            f"{checksums[field][name]} != {checksum}"
                        )
                    checksums[field][name] = checksum

    def merge(self, other: Changes) -> Changes:
        """Merge another .changes file into this one."""
        self._merge_check_simple_fields(other)
        self._merge_check_descriptions(other)
        self._merge_check_checksums(other)

        merged = Changes(str(self))
        merged.merge_fields("Binary", other)
        merged.merge_fields("Architecture", other)
        merged.merge_fields("Description", other)

        for field in ("Files", "Checksums-Sha1", "Checksums-Sha256"):
            existing = {tuple(checksum.items()) for checksum in merged[field]}
            for checksum in other[field]:
                assert tuple(checksum.items()) not in existing
                merged[field].append(checksum)

        merged.order_before("Binary", "Source")
        merged.order_before("Description", "Changes")

        return merged


class BuildInfo(_gpg_multivalued, _PkgRelationMixin, _VersionAccessorMixin):
    """ Representation of a .buildinfo (build environment description) file

    This class is a thin wrapper around the transparent GPG handling
    of :class:`_gpg_multivalued`, the field parsing of
    :class:`_PkgRelationMixin`,
    and the format parsing of :class:`Deb822`.

    Note that the 'relations' structure returned by the `relations` method
    is identical to that produced by other classes in this module.
    Consequently, existing code to consume this structure can be used here,
    although it means that there are redundant lists and tuples within the
    structure.

    Example::

        >>> from debian.deb822 import BuildInfo
        >>> filename = 'package.buildinfo'
        >>> with open(filename) as fh:    # doctest: +SKIP
        ...     info = BuildInfo(fh)
        >>> print(info.get_environment())    # doctest: +SKIP
        {'DEB_BUILD_OPTIONS': 'parallel=4',
        'LANG': 'en_AU.UTF-8',
        'LC_ALL': 'C.UTF-8',
        'LC_TIME': 'en_GB.UTF-8',
        'LD_LIBRARY_PATH': '/usr/lib/libeatmydata',
        'SOURCE_DATE_EPOCH': '1601784586'}
        >>> installed = info.relations['installed-build-depends']  # doctest: +SKIP
        >>> for dep in installed:  # doctest: +SKIP
        ...     print("Installed %s/%s" % (dep[0]['name'], dep[0]['version'][1]))
        Installed autoconf/2.69-11.1
        Installed automake/1:1.16.2-4
        Installed autopoint/0.19.8.1-10
        Installed autotools-dev/20180224.1
        ... etc ...
        >>> changelog = info.get_changelog() # doctest: +SKIP
        >>> print(changelog.author) # doctest: +SKIP
        'xyz Build Daemon (xyz-01) <buildd_xyz-01@buildd.debian.org>'
        >>> print(changelog[0].changes()) # doctest: +SKIP
        ['',
        '  * Binary-only non-maintainer upload for amd64; no source changes.',
        '  * Add Python 3.9 as supported version',
        '']
    """
    _multivalued_fields = {
        "checksums-md5": ["md5", "size", "name"],
        "checksums-sha1": ["sha1", "size", "name"],
        "checksums-sha256": ["sha256", "size", "name"],
        "checksums-sha512": ["sha512", "size", "name"],
    }
    _relationship_fields = [
        'installed-build-depends',
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _gpg_multivalued.__init__(self, *args, **kwargs)
        _PkgRelationMixin.__init__(self, *args, **kwargs)

    def _get_array_value(self, field: str) -> list[str] | None:
        if field not in self:
            raise KeyError(f"'{field}' not found in buildinfo")
        return list(self[field].replace('\n', '').strip().split())

    def get_environment(self) -> dict[str, str]:
        """Return the build environment that was recorded

        The environment is returned as a dict in the style of `os.environ`.
        The backslash quoting of values described in deb-buildinfo(5) is
        removed.
        """

        #if 'Environment' not in self:
            #return {}

        return dict(BuildInfo._env_deserialise(self.get('Environment', '')))

    def get_changelog(self) -> debian.changelog.Changelog | None:
        """Return the changelog entry from the buildinfo (for binNMUs)

        If no "Binary-Only-Changes" field is present in the buildinfo file
        then `None` is returned.
        """

        if 'Binary-Only-Changes' not in self:
            return None

        # remove the indentation and . that is applied to the changelog
        chlines = self['Binary-Only-Changes'].splitlines()
        chlines = ['' if s == ' .' else s[1:] for s in chlines]
        return debian.changelog.Changelog(chlines)

    def get_source(self) -> tuple[str, str]:
        if 'source' not in self:
            raise KeyError("'Source' field not found in buildinfo")
        matches = self._explicit_source_re.match(self['source'])
        if not matches:
            raise ValueError("Invalid 'Source' field specified")
        return matches.group('source'), matches.group('version')

    def get_binary(self) -> list[str] | None:
        return self._get_array_value('Binary')

    def get_build_date(self) -> datetime.datetime:
        if 'build-date' not in self:
            raise KeyError("'Build-Date' field not found in buildinfo")
        timearray = email.utils.parsedate_tz(self['build-date'])
        if timearray is None:
            raise ValueError("Invalid 'Build-Date' field specified")
        ts = email.utils.mktime_tz(timearray)
        return datetime.datetime.fromtimestamp(ts)

    def get_architecture(self) -> list[str] | None:
        return self._get_array_value('Architecture')

    def is_build_source(self) -> bool:
        arches = [arch for arch in self.get_architecture()  # type: ignore
                  if arch == "source"]
        return len(arches) == 1

    def is_build_arch_all(self) -> bool:
        return 'all' in self.get_architecture()  # type: ignore

    def is_build_arch_any(self) -> bool:
        arches = [arch for arch in self.get_architecture()  # type: ignore
                  if arch not in ("all", "source")]
        return len(arches) == 1

    class _EnvParserState():
        # trivial enum for the deserialiser
        IGNORE_WHITESPACE = 0
        VAR_NAME = 1
        START_VALUE_QUOTE = 2
        VALUE = 3
        VALUE_BACKSLASH_ESCAPE = 4

    @staticmethod
    def _env_deserialise(serialised: str) -> Generator[tuple[str, str]]:
        """ extract the environment variables and values from the text

        Format is:
            VAR_NAME="value"

        with ignorable whitespace around the construct (and separating each
        item). Quote characters within the value are backslash escaped.

        When producing the buildinfo file, dpkg only includes specifically
        allowed environment variables and thus there is no defined quoting
        rules for the variable names.

        The format is described by deb-buildinfo(5) and implemented in
        dpkg source scripts/dpkg-genbuildinfo.pl:cleansed_environment(),
        while the environment variables that are included in the output are
        listed in dpkg source scripts/Dpkg/Build/Info.pm
        """
        # The deserialiser is implemented as a state machine with the
        # states listed in _EnvParserState.
        state = BuildInfo._EnvParserState.IGNORE_WHITESPACE
        name = ""
        value: str | None = None

        for ch in serialised:
            if state == BuildInfo._EnvParserState.IGNORE_WHITESPACE:
                if not ch.isspace():
                    state = BuildInfo._EnvParserState.VAR_NAME
                    name = ch
                continue

            if state == BuildInfo._EnvParserState.VAR_NAME:
                if ch != "=":
                    name += ch
                else:
                    state = BuildInfo._EnvParserState.START_VALUE_QUOTE
                    value = ""
                continue

            if state == BuildInfo._EnvParserState.START_VALUE_QUOTE:
                if ch == '"':
                    state = BuildInfo._EnvParserState.VALUE
                else:
                    raise ValueError(
                        "Improper quoting in Environment: "
                        "begin quote not found"
                    )
                continue

            if state == BuildInfo._EnvParserState.VALUE:
                if ch == "\\":
                    state = BuildInfo._EnvParserState.VALUE_BACKSLASH_ESCAPE
                elif ch == '"':
                    if name == "":
                        raise ValueError(
                            "Improper formatting in Environment: "
                            "variable name not found"
                        )
                    if value is None:
                        raise ValueError(
                            "Improper formatting in Environment: "
                            "variable value not found"
                        )

                    yield name, value

                    state = BuildInfo._EnvParserState.IGNORE_WHITESPACE
                    name = ""
                    value = None
                else:
                    assert value is not None
                    value += ch
                continue

            if state == BuildInfo._EnvParserState.VALUE_BACKSLASH_ESCAPE:
                if ch == '"':
                    assert value is not None
                    value += ch
                    state = BuildInfo._EnvParserState.VALUE
                else:
                    raise ValueError(
                        "Improper formatting in Environment: "
                        "couldn't interpret backslash sequence"
                    )
                continue

        if state != BuildInfo._EnvParserState.IGNORE_WHITESPACE:
            raise ValueError(
                "Improper quoting in Environment: "
                "end quote not found"
            )


class PdiffIndex(_multivalued):
    """ Representation of a foo.diff/Index file from a Debian mirror

    This class is a thin wrapper around the transparent GPG handling
    of :class:`_gpg_multivalued` and the parsing of :class:`Deb822`.
    """
    _multivalued_fields = {
        "sha1-current": ["SHA1", "size"],
        "sha1-history": ["SHA1", "size", "date"],
        "sha1-patches": ["SHA1", "size", "date"],
        "sha1-download": ["SHA1", "size", "filename"],
        "x-unmerged-sha1-history": ["SHA1", "size", "date"],
        "x-unmerged-sha1-patches": ["SHA1", "size", "date"],
        "x-unmerged-sha1-download": ["SHA1", "size", "filename"],
        "sha256-current": ["SHA256", "size"],
        "sha256-history": ["SHA256", "size", "date"],
        "sha256-patches": ["SHA256", "size", "date"],
        "sha256-download": ["SHA256", "size", "filename"],
        "x-unmerged-sha256-history": ["SHA256", "size", "date"],
        "x-unmerged-sha256-patches": ["SHA256", "size", "date"],
        "x-unmerged-sha256-download": ["SHA256", "size", "filename"],
    }

    @property
    def _fixed_field_lengths(self) -> dict[str, dict[str, int]]:
        fixed_field_lengths: dict[str, dict[str, int]] = {}
        for key in self._multivalued_fields:
            if hasattr(self[key], 'keys'):
                # Not multi-line -- don't need to compute the field length for
                # this one
                continue
            length = self._get_size_field_length(key)
            fixed_field_lengths[key] = {"size": length}
        return fixed_field_lengths

    def _get_size_field_length(self, key: str) -> int:
        lengths = [len(str(item['size'])) for item in self[key]]
        return max(lengths)


class Release(_multivalued):
    """Represents a Release file

    Set the size_field_behavior attribute to "dak" to make the size field
    length only as long as the longest actual value.  The default,
    "apt-ftparchive" makes the field 16 characters long regardless.

    This class is a thin wrapper around the parsing of :class:`Deb822`.
    """
    # FIXME: Add support for detecting the behavior of the input, if
    # constructed from actual 822 text.

    _multivalued_fields = {
        "md5sum": ["md5sum", "size", "name"],
        "sha1": ["sha1", "size", "name"],
        "sha256": ["sha256", "size", "name"],
        "sha512": ["sha512", "size", "name"],
    }

    __size_field_behavior = "apt-ftparchive"

    def set_size_field_behavior(self, value: str) -> None:
        if value not in ["apt-ftparchive", "dak"]:
            raise ValueError("size_field_behavior must be either "
                             "'apt-ftparchive' or 'dak'")

        self.__size_field_behavior = value

    size_field_behavior = property(lambda self: self.__size_field_behavior,
                                   set_size_field_behavior)

    @property
    def _fixed_field_lengths(self) -> dict[str, dict[str, int]]:
        fixed_field_lengths: dict[str, dict[str, int]] = {}
        for key in self._multivalued_fields:
            length = self._get_size_field_length(key)
            fixed_field_lengths[key] = {"size": length}
        return fixed_field_lengths

    def _get_size_field_length(self, key: str) -> int:
        if self.size_field_behavior == "apt-ftparchive":
            return 16
        if self.size_field_behavior == "dak":
            lengths = [len(str(item['size'])) for item in self[key]]
            return max(lengths)
        raise ValueError("Illegal value for size_field_behavior")


class Sources(Dsc, _PkgRelationMixin):
    """Represent an APT source package list

    This class is a thin wrapper around the parsing of :class:`Deb822`,
    using the field parsing of :class:`_PkgRelationMixin`.
    """
    _relationship_fields = [
        'build-depends', 'build-depends-indep', 'build-depends-arch',
        'build-conflicts', 'build-conflicts-indep', 'build-conflicts-arch',
        'binary',
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        Dsc.__init__(self, *args, **kwargs)
        _PkgRelationMixin.__init__(self, *args, **kwargs)

    @classmethod
    def iter_paragraphs(cls,
                        sequence: InputDataType | Path,
                        fields: list[str] | None = None,
                        use_apt_pkg: bool = True,
                        shared_storage: bool = False,
                        encoding: str = "utf-8",
                        strict: dict[str, bool] | None = None,
                       ) -> Iterator[Sources]:
        """Generator that yields a Deb822 object for each paragraph in Sources.

        Note that this overloaded form of the generator uses apt_pkg (a strict
        but fast parser) by default.

        See the :func:`~Deb822.iter_paragraphs` function for details.
        """
        if not strict:
            strict = {
                'whitespace-separates-paragraphs': False,
            }
        return super().iter_paragraphs(
            sequence, fields, use_apt_pkg, shared_storage, encoding, strict)


class Packages(Deb822, _PkgRelationMixin, _VersionAccessorMixin):
    """Represent an APT binary package list

    This class is a thin wrapper around the parsing of :class:`Deb822`,
    using the field parsing of :class:`_PkgRelationMixin`.
    """
    _relationship_fields = [
        'depends', 'pre-depends', 'recommends', 'suggests',
        'breaks', 'conflicts', 'provides', 'replaces',
        'enhances', 'built-using',
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        Deb822.__init__(self, *args, **kwargs)
        _PkgRelationMixin.__init__(self, *args, **kwargs)

    @classmethod
    def iter_paragraphs(cls,
                        sequence: InputDataType | Path,
                        fields: list[str] | None = None,
                        use_apt_pkg: bool = True,
                        shared_storage: bool = False,
                        encoding: str = "utf-8",
                        strict: dict[str, bool] | None = None,
                       ) -> Iterator[Packages]:
        """Generator that yields a Deb822 object for each paragraph in Packages.

        Note that this overloaded form of the generator uses apt_pkg (a strict
        but fast parser) by default.

        See the :func:`~Deb822.iter_paragraphs` function for details.
        """
        if not strict:
            strict = {
                'whitespace-separates-paragraphs': False,
            }
        return super().iter_paragraphs(
            sequence, fields, use_apt_pkg, shared_storage, encoding, strict)

    @property
    def source(self) -> str | None:
        """ source package that generates the binary package

        If the source package and source package version are the same as the
        binary package, an explicit "Source" field will not be within the
        paragraph.
        """
        if 'source' not in self:
            return self['package']   # type: ignore

        matches = self._explicit_source_re.match(self['source'])
        if matches:
            return matches.group('source')
        return None

    @property
    def source_version(self) -> debian.debian_support.Version:
        """ source package that generates the binary package

        If the source package and source package version are the same as the
        binary package, an explicit "Source" field will not be within the
        paragraph.
        """
        if 'source' not in self:
            return self.get_version()

        matches = self._explicit_source_re.match(self['source'])
        if matches and matches.group('version'):
            version = matches.group('version')
        else:
            version = self['Version']
        return debian.debian_support.Version(version)


class DebControl(Packages):
    """Represent an Debian binary package control file

    This class is a thin wrapper around the parsing of :class:`Deb822`,
    using the field parsing of :class:`_PkgRelationMixin`.
    """


class RestrictedField(collections.namedtuple(
        'RestrictedField', 'name from_str to_str allow_none')):
    """Placeholder for a property providing access to a restricted field.

    Use this as an attribute when defining a subclass of RestrictedWrapper.
    It will be replaced with a property.  See the RestrictedWrapper
    documentation for an example.
    """

    def __new__(cls,
                name: str,
                from_str: Callable[[str], Any] | None = None,
                to_str: Callable[[Any], str | None] | None = None,
                allow_none: bool | None = True,
                ) -> RestrictedField:
        """Create a new RestrictedField placeholder.

        The getter that will replace this returns (or applies the given to_str
        function to) None for fields that do not exist in the underlying data
        object.

        :param name: The name of the deb822 field.
        :param from_str: The function to apply for getters (default is to
            return the string directly).
        :param to_str: The function to apply for setters (default is to use the
            value directly).  If allow_none is True, this function may return
            None, in which case the underlying key is deleted.
        :param allow_none: Whether it is allowed to set the value to None
            (which results in the underlying key being deleted).
        """
        return super().__new__(
            cls, name, from_str=from_str, to_str=to_str,
            allow_none=allow_none)

class Removals(Deb822):
    """Represent an ftp-master removals.822 file

    Removal of packages from the archive are recorded by ftp-masters.
    See https://ftp-master.debian.org/#removed

    Note: this API is experimental and backwards-incompatible changes might be
    required in the future. Please use it and help us improve it!
    """
    __sources_line_re = re.compile(
        r'\s*'
        r'(?P<package>.+?)'
        r'_'
        r'(?P<version>[^\s]+)'
        r'\s*'
    )
    __binaries_line_re = re.compile(
        r'\s*'
        r'(?P<package>.+?)'
        r'_'
        r'(?P<version>[^\s]+)'
        r'\s+'
        r'\[(?P<archs>.+)\]'
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._sources: list[dict[str, str]] | None = None
        self._binaries: list[dict[str, str | Iterable[str]]] | None = None

    @property
    def date(self) -> datetime.datetime:
        """ a datetime object for the removal action """
        timearray = email.utils.parsedate_tz(self['date'])
        if timearray is None:
            raise ValueError("No date specified")
        ts = email.utils.mktime_tz(timearray)
        return datetime.datetime.fromtimestamp(ts)

    @property
    def bug(self) -> list[int]:
        """ list of bug numbers that had requested the package removal

        The bug numbers are returned as integers.

        Note: there is normally only one entry in this list but there may be
        more than one.
        """
        if 'bug' not in self:
            return []
        return [int(b) for b in self['bug'].split(",")]

    @property
    def also_wnpp(self) -> list[int]:
        """ list of WNPP bug numbers closed by the removal

        The bug numbers are returned as integers.
        """
        if 'also-wnpp' not in self:
            return []
        return [int(b) for b in self['also-wnpp'].split(" ")]

    @property
    def also_bugs(self) -> list[int]:
        """ list of bug numbers in the package closed by the removal

        The bug numbers are returned as integers.

        Removal of a package implicitly also closes all bugs associated with
        the package.
        """
        if 'also-bugs' not in self:
            return []
        return [int(b) for b in self['also-bugs'].split(" ")]

    @property
    def sources(self) -> list[dict[str, str]]:
        """ list of source packages that were removed

        A list of dicts is returned, each dict has the form::

            {
                'source': 'some-package-name',
                'version': '1.2.3-1'
            }

        Note: There may be no source packages removed at all if the removal is
        only of a binary package. An empty list is returned in that case.
        """
        if self._sources is not None:
            return self._sources

        s: list[dict[str, str]] = []
        if 'sources' in self:
            for line in self['sources'].splitlines():
                matches = self.__sources_line_re.match(line)
                if matches:
                    s.append(
                        {
                            'source': matches.group('package'),
                            'version': matches.group('version'),
                        })
        self._sources = s
        return s

    @property
    def binaries(self) -> list[dict[str, str | Iterable[str]]]:
        """ list of binary packages that were removed

        A list of dicts is returned, each dict has the form::

            {
                'package': 'some-package-name',
                'version': '1.2.3-1',
                'architectures': set(['i386', 'amd64'])
            }
        """
        if self._binaries is not None:
            return self._binaries

        b: list[dict[str, str | Iterable[str]]] = []
        if 'binaries' in self:
            for line in self['binaries'].splitlines():
                matches = self.__binaries_line_re.match(line)
                if matches:
                    b.append({
                        'package': matches.group('package'),
                        'version': matches.group('version'),
                        'architectures':
                            set(matches.group('archs').split(', ')),
                    })
        self._binaries = b
        return b


class _AutoDecoder:

    def __init__(self, encoding: str | None = None) -> None:
        self.encoding = encoding or 'UTF-8'

    def decode(self, value: str | bytes) -> str:
        """If value is not already Unicode, decode it intelligently."""
        if isinstance(value, bytes):
            return self.decode_bytes(value)
        return value

    def decode_bytes(self, value: bytes) -> str:
        try:
            return value.decode(self.encoding)
        except UnicodeDecodeError as e:
            if not _have_charset_normalizer and not _have_chardet:
                logger.error(
                    'decoding from %s failed; please install charset_normalizer or '
                    'chardet for automated detection of encoding',
                    self.encoding
                )
                raise e

            # Evidently, the value wasn't encoded with the encoding the
            # user specified.  Try detecting it.
            logger.warning('decoding from %s failed; attempting to detect '
                           'the true encoding', self.encoding)

            encoding: str | None = None
            if _have_charset_normalizer:
                # Try detect few encodings, not using full charset_normalizer
                # capabilities as there are many encodings which are unlikely
                # (for example Mac or Windows specific)
                result = charset_normalizer.from_bytes(
                    value,
                    cp_isolation=[f"iso-8859-{n}" for n in (1, 2, 7, 8, 9)]
                ).best()
                if result:
                    encoding = result.encoding
            else:  # try fallback to chardet
                result_cd = chardet.detect(value)   # pylint: disable=used-before-assignment
                encoding = result_cd['encoding']
            if encoding is None:
                raise
            try:
                decoded = value.decode(encoding)
                # Assume the rest of the paragraph is in this encoding as
                # well (there's no sense in repeating this exercise for
                # every field).
                self.encoding = encoding
                return decoded
            except UnicodeDecodeError:
                raise e
