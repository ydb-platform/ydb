""" Facilities to deal with Debian-specific metadata """

# Copyright (C) 2005       Florian Weimer <fw@deneb.enyo.de>
# Copyright (C) 2010       John Wright <jsw@debian.org>
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

from __future__ import annotations

import os
import os.path
import re

from typing import (
    Any,
    AnyStr,
    BinaryIO,
    Iterable,
    Iterator,
    Generator,
    Match,
    NoReturn,
    Pattern,
    TextIO,
)

try:
    import apt_pkg
    try:
        apt_pkg.init()
        _have_apt_pkg = True
    except apt_pkg.Error:
        # If dpkg (e.g., tupledata) is missing, we can import apt_pkg but .init()
        # will raise an exception
        _have_apt_pkg = False
except ImportError:
    _have_apt_pkg = False

# Use the built-in _sha extension instead of hashlib to avoid a dependency on
# OpenSSL, which is incompatible with the GPL.
try:
    import _sha1    # type: ignore
    new_sha1 = _sha1.sha1
except ImportError:
    def new_sha1(*args: bytes) -> str:    # pylint: disable=unused-argument
        raise NotImplementedError(
            "Built-in sha1 implementation not found; cannot use hashlib"
            " implementation because it depends on OpenSSL, which"
            " may not be linked with this library due to license"
            " incompatibilities")


# Use the built-in _sha256 extension instead of hashlib to avoid a dependency on
# OpenSSL, which is incompatible with the GPL.
try:
    import _sha256    # type: ignore
    new_sha256 = _sha256.sha256
except ImportError:
    def new_sha256(*args: bytes) -> str:    # pylint: disable=unused-argument
        raise NotImplementedError(
            "Built-in sha1 implementation not found; cannot use hashlib"
            " implementation because it depends on OpenSSL, which"
            " may not be linked with this library due to license"
            " incompatibilities")


# Re-exports
import debian._arch_table

DpkgArchTable = debian._arch_table.DpkgArchTable


class ParseError(Exception):
    """An exception which is used to signal a parse failure.

    Attributes:

    filename - name of the file
    lineno - line number in the file
    msg - error message

    """

    def __init__(self,
                 filename: str,
                 lineno: int,
                 msg: str
                 ) -> None:
        assert isinstance(lineno, int)
        self.filename = filename
        self.lineno = lineno
        self.msg = msg
        super().__init__(self)

    def __str__(self) -> str:
        return self.msg

    def __repr__(self) -> str:
        return "ParseError(%r, %d, %r)" % (self.filename,
                                           self.lineno,
                                           self.msg)

    def print_out(self, file: TextIO) -> None:
        """Writes a machine-parsable error message to file."""
        file.write("%s:%d: %s\n" % (self.filename, self.lineno, self.msg))
        file.flush()


class BaseVersion:
    """Base class for classes representing Debian versions

    It doesn't implement any comparison, but it does check for valid versions
    according to Section 5.6.12 in the Debian Policy Manual.  Since splitting
    the version into epoch, upstream_version, and debian_revision components is
    pretty much free with the validation, it sets those fields as properties of
    the object, and sets the raw version to the full_version property.  A
    missing epoch or debian_revision results in the respective property set to
    None.  Setting any of the properties results in the full_version being
    recomputed and the rest of the properties set from that.

    It also implements __str__, just returning the raw version given to the
    initializer.
    """

    re_valid_version = re.compile(
        r"^((?P<epoch>\d+):)?"
        "(?P<upstream_version>[A-Za-z0-9.+:~-]+?)"
        "(-(?P<debian_revision>[A-Za-z0-9+.~]+))?$")
    magic_attrs = (
        'full_version', 'epoch', 'upstream_version',
        'debian_revision', 'debian_version')

    def __init__(self, version: str | BaseVersion | None) -> None:
        if isinstance(version, BaseVersion):
            version = str(version)
        self.full_version = version

    def _set_full_version(self, version: str) -> None:
        m = self.re_valid_version.match(version)
        if not m:
            raise ValueError("Invalid version string %r" % version)
        # If there no epoch ("1:..."), then the upstream version can not
        # contain a :.
        if m.group("epoch") is None and ":" in m.group("upstream_version"):
            raise ValueError("Invalid version string %r" % version)

        # pylint: disable=attribute-defined-outside-init
        self.__full_version = version  # pylint: disable = unused-private-member
        self.__epoch = m.group("epoch")
        self.__upstream_version = m.group("upstream_version")
        self.__debian_revision = m.group("debian_revision")

    def __setattr__(self, attr: str, value: str | None) -> None:
        if attr not in self.magic_attrs:
            super().__setattr__(attr, value)
            return

        # For compatibility with the old changelog.Version class
        if attr == "debian_version":
            attr = "debian_revision"

        if attr == "full_version":
            self._set_full_version(str(value))
        else:
            if value is not None:
                value = str(value)
            private = "_BaseVersion__%s" % attr
            old_value = getattr(self, private)
            setattr(self, private, value)
            try:
                self._update_full_version()
            except ValueError:
                # Don't leave it in an invalid state
                setattr(self, private, old_value)
                self._update_full_version()
                raise ValueError("Setting %s to %r results in invalid version"
                                 % (attr, value))

    def __getattr__(self, attr: str) -> str | None:
        if attr not in self.magic_attrs:
            return super().__getattribute__(attr) # type: ignore

        # For compatibility with the old changelog.Version class
        if attr == "debian_version":
            attr = "debian_revision"

        private = "_BaseVersion__%s" % attr
        return getattr(self, private)  # type: ignore

    def _update_full_version(self) -> None:
        version = ""
        if self.__epoch is not None:
            version += self.__epoch + ":"
        version += self.__upstream_version
        if self.__debian_revision:
            version += "-" + self.__debian_revision
        self.full_version = version

    def __str__(self) -> str:
        return self.full_version if self.full_version is not None else ""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self}')"

    def _compare(self, other: Any) -> int:
        raise NotImplementedError

    # TODO: Once we support only Python >= 2.7, we can simplify this using
    # @functools.total_ordering.

    def __lt__(self, other: Any) -> bool:
        return self._compare(other) < 0

    def __le__(self, other: Any) -> bool:
        return self._compare(other) <= 0

    def __eq__(self, other: Any) -> bool:
        return self._compare(other) == 0

    def __ne__(self, other: Any) -> bool:
        return self._compare(other) != 0

    def __ge__(self, other: Any) -> bool:
        return self._compare(other) >= 0

    def __gt__(self, other: Any) -> bool:
        return self._compare(other) > 0

    def __hash__(self) -> int:
        return hash(str(self))


class AptPkgVersion(BaseVersion):
    """Represents a Debian package version, using apt_pkg.VersionCompare"""

    def __init__(self, version: str | BaseVersion | None) -> None:
        if not _have_apt_pkg:
            raise NotImplementedError("apt_pkg not available; install the "
                                      "python-apt package")
        super().__init__(version)

    def _compare(self, other: Any) -> int:
        return apt_pkg.version_compare(str(self), str(other))


# NativeVersion based on the DpkgVersion class by Raphael Hertzog in
# svn://svn.debian.org/qa/trunk/pts/www/bin/common.py r2361
class NativeVersion(BaseVersion):
    """Represents a Debian package version, with native Python comparison"""

    re_all_digits_or_not = re.compile(r"\d+|\D+")
    re_digits = re.compile(r"\d+")
    re_digit = re.compile(r"\d")
    re_alpha = re.compile("[A-Za-z]")

    def _compare(self, other: Any) -> int:
        # Convert other into an instance of BaseVersion if it's not already.
        # (All we need is epoch, upstream_version, and debian_revision
        # attributes, which BaseVersion gives us.) Requires other's string
        # representation to be the raw version.

        # If other is not defined, then the current version is bigger
        if other is None:
            return 1

        if not isinstance(other, BaseVersion):
            try:
                other = BaseVersion(str(other))
            except ValueError as e:
                raise ValueError("Couldn't convert %r to BaseVersion: %s"
                                 % (other, e))

        lepoch = int(self.epoch or "0")
        repoch = int(other.epoch or "0")
        if lepoch < repoch:
            return -1
        if lepoch > repoch:
            return 1
        res = self._version_cmp_part(self.upstream_version or "0",
                                     other.upstream_version or "0")
        if res != 0:
            return res
        return self._version_cmp_part(self.debian_revision or "0",
                                      other.debian_revision or "0")

    @classmethod
    def _order(cls, x: str) -> int:
        """Return an integer value for character x"""
        if x == '~':
            return -1
        if cls.re_digit.match(x):
            return int(x) + 1
        if cls.re_alpha.match(x):
            return ord(x)
        return ord(x) + 256

    @classmethod
    def _version_cmp_string(cls, va: str, vb: str) -> int:
        la = [cls._order(x) for x in va]
        lb = [cls._order(x) for x in vb]
        while la or lb:
            a = 0
            b = 0
            if la:
                a = la.pop(0)
            if lb:
                b = lb.pop(0)
            if a < b:
                return -1
            if a > b:
                return 1
        return 0

    @classmethod
    def _version_cmp_part(cls, va: str, vb: str) -> int:
        la = cls.re_all_digits_or_not.findall(va)
        lb = cls.re_all_digits_or_not.findall(vb)
        while la or lb:
            a = "0"
            b = "0"
            if la:
                a = la.pop(0)
            if lb:
                b = lb.pop(0)
            if cls.re_digits.match(a) and cls.re_digits.match(b):
                aval = int(a)
                bval = int(b)
                if aval < bval:
                    return -1
                if aval > bval:
                    return 1
            else:
                res = cls._version_cmp_string(a, b)
                if res != 0:
                    return res
        return 0


if _have_apt_pkg:
    class Version(AptPkgVersion):
        pass
else:
    class Version(NativeVersion):      # type: ignore
        pass


def version_compare(a: Any, b: Any) -> int:
    va = Version(a)
    vb = Version(b)
    if va < vb:
        return -1
    if va > vb:
        return 1
    return 0


class PackageFile:
    """A Debian package file.

    Objects of this class can be used to read Debian's Source and
    Packages files."""

    re_field = re.compile(r'^([A-Za-z][A-Za-z0-9-_]+):(?:\s*(.*?))?\s*$')
    re_continuation = re.compile(r'^\s+(?:\.|(\S.*?)\s*)$')

    def __init__(self,
                 name: str,
                 file_obj: TextIO | BinaryIO | None = None,
                 encoding: str = "utf-8",
                 ) -> None:
        """Creates a new package file object.

        name - the name of the file the data comes from
        file_obj - an alternate data source; the default is to open the
                  file with the indicated name.
        """
        if file_obj is None:
            file_obj = open(name, 'rb')  # pylint: disable=consider-using-with
        self.name = name
        self.file = file_obj
        self.lineno = 0
        self.encoding = encoding

    def __iter__(self) -> Generator[list[tuple[str, str]]]:
        line = self._aux_read_line()
        self.lineno += 1
        pkg: list[tuple[str, str]] = []
        while line:
            if line.strip(' \t') == '\n':
                if not pkg:
                    self.raise_syntax_error('expected package record')
                yield pkg
                pkg = []
                line = self._aux_read_line()
                self.lineno += 1
                continue

            match: Match[str] | None = self.re_field.match(line)
            if not match:
                self.raise_syntax_error("expected package field")
            (name, contents) = match.groups()
            contents = contents or ''

            while True:
                line = self._aux_read_line()
                self.lineno += 1
                match = self.re_continuation.match(line)
                if match:
                    (ncontents,) = match.groups()
                    if ncontents is None:
                        ncontents = ""
                    contents = f"{contents}\n{ncontents}"
                else:
                    break
            pkg.append((name, contents))
        if pkg:
            yield pkg

    def _aux_read_line(self) -> str:
        # Not always readline returns a byte object, also str
        # can be returned (i.e: StringIO)
        line = self.file.readline()
        if isinstance(line, bytes):
            return line.decode(self.encoding)
        return line

    def raise_syntax_error(self, msg: str, lineno: int | None = None) -> NoReturn:
        if lineno is None:
            lineno = self.lineno
        raise ParseError(self.name, lineno, msg)


class PseudoEnum:
    """A base class for types which resemble enumeration types."""
    def __init__(self,
                 name: str,
                 order: Any,
                 ):
        self._name = name
        self._order = order

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._name!r})'

    def __str__(self) -> str:
        return self._name

    # TODO: Once we support only Python >= 2.7, we can simplify this using
    # @functools.total_ordering.

    def __lt__(self, other: Any) -> Any:
        return self._order < other._order

    def __le__(self, other: Any) -> Any:
        return self._order <= other._order

    def __eq__(self, other: Any) -> Any:
        return self._order == other._order

    def __ne__(self, other: Any) -> Any:
        return self._order != other._order

    def __ge__(self, other: Any) -> Any:
        return self._order >= other._order

    def __gt__(self, other: Any) -> Any:
        return self._order > other._order

    def __hash__(self) -> int:
        return hash(self._order)


class Release(PseudoEnum):
    """
    Debian release defined with respect to its name, order of release
    and version. The latter can be empty in case of 'sid'.

    See https://www.debian.org/releases/
    """
    releases: dict[str, Release] = {}

    def __init__(self,
                 name: str,
                 order: Any,
                 version: str = ""
                 ):
        super().__init__(name, order)
        self.version = version


def list_releases() -> dict[str, Release]:
    """
     Returns dict of Debian releases
    """
    releases = {}
    rels = (("buzz", "1.1"),
            ("rex", "1.2"),
            ("bo", "1.3"),
            ("hamm", "2.0"),
            ("slink", "2.1"),
            ("potato", "2.2"),
            ("woody", "3.0"),
            ("sarge", "3.1"),
            ("etch", "4.0"),
            ("lenny", "5.0"),
            ("squeeze", "6.0"),
            ("wheezy", "7"),
            ("jessie", "8"),
            ("stretch", "9"),
            ("buster", "10"),
            ("bullseye", "11"),
            ("bookworm", "12"),
            ("trixie", "13"),
            ("forky", "14"),
            ("sid", ""))
    for idx, rel in enumerate(rels):
        name, version = rel
        releases[name] = Release(name, idx, version)
    Release.releases = releases
    return releases


_release_list = list_releases()


def intern_release(name: str, releases: Any | None = None) -> Any:
    if releases is None:
        releases = _release_list
    return releases.get(name)


del list_releases


def read_lines_sha256(lines: list[bytes] | list[str]) -> str:
    m = new_sha256()
    for l in lines:
        if isinstance(l, bytes):
            m.update(l)
        else:
            m.update(l.encode("UTF-8"))
    return m.hexdigest()   # type: ignore


def read_lines_sha1(lines: list[bytes] | list[str]) -> str:
    m = new_sha1()
    for l in lines:
        if isinstance(l, bytes):
            m.update(l)
        else:
            m.update(l.encode("UTF-8"))
    return m.hexdigest()   # type: ignore


_patch_re_raw = r'^(\d+)(?:,(\d+))?([acd])$'
_patch_re: Pattern[str] = re.compile(_patch_re_raw)
_patch_re_b: Pattern[bytes] = re.compile(_patch_re_raw.encode('UTF-8'))


def patches_from_ed_script(
        source: Iterable[AnyStr],
        re_cmd: Pattern[AnyStr] | None = None,
    ) -> Iterator[tuple[int, int, list[AnyStr]]]:
    """Converts source to a stream of patches.

    Patches are triples of line indexes:

    - number of the first line to be replaced
    - one plus the number of the last line to be replaced
    - list of line replacements

    This is enough to model arbitrary additions, deletions and
    replacements.
    """

    i = iter(source)

    patch_re = re_cmd

    for line in i:
        if not patch_re:
            patch_re = _patch_re_b if isinstance(line, bytes) else _patch_re

        match = patch_re.match(line)
        if match is None:
            raise ValueError("invalid patch command: %r" % line)

        (first_, last_, cmd) = match.groups()
        first = int(first_)
        last = None if last_ is None else int(last_)

        # using ord() makes this work for str and bytes objects
        if ord(cmd) == 100: # cmd == d
            first = first - 1
            if last is None:
                last = first + 1
            yield (first, last, [])
            continue

        if ord(cmd) == 97: # cmd == a
            if last is not None:
                raise ValueError("invalid patch argument: %r" % line)
            last = first
        else:                           # cmd == c
            first = first - 1
            if last is None:
                last = first + 1

        lines = []
        for c in i:
            if c in ('', b''):
                raise ValueError("end of stream in command: %r" % line)
            if c in ('.\n', '.', b'.\n', b'.'):
                break
            lines.append(c)
        yield (first, last, lines)


def patch_lines(
        lines: list[AnyStr],
        patches: Iterable[tuple[int, int, list[AnyStr]]],
    ) -> None:
    """Applies patches to lines.  Updates lines in place."""
    for (first, last, args) in patches:
        lines[first:last] = args


def replace_file(lines: list[str], local: str, encoding: str = "UTF-8") -> None:
    local_new = local + '.new'

    try:
        with open(local_new, 'w+', encoding=encoding) as new_file:
            for l in lines:
                new_file.write(l)
        os.replace(local_new, local)
    finally:
        if os.path.exists(local_new):
            os.unlink(local_new)


def download_gunzip_lines(remote: str) -> list[str]:
    """Downloads a file from a remote location and gunzips it.

    Returns the lines in the file."""
    # pylint: disable=import-outside-toplevel
    import gzip
    from urllib.request import urlopen

    with urlopen(remote) as zfd:
        with gzip.open(zfd, mode="rt") as gfd:
            return gfd.readlines()   # type: ignore


def download_file(remote: str, local: str) -> list[str]:
    """Copies a gzipped remote file to the local system.

    remote - URL, without the .gz suffix
    local - name of the local file
    """

    lines = download_gunzip_lines(remote + '.gz')
    replace_file(lines, local)
    return lines


def update_file(remote: str, local: str, verbose: bool = False) -> list[str]:
    """Updates the local file by downloading a remote patch.

    Returns a list of lines in the local file.
    """

    try:
        with open(local, encoding="UTF-8") as local_file:
            lines = local_file.readlines()
    except OSError:
        if verbose:
            print("update_file: no local copy, downloading full file")
        return download_file(remote, local)

    patches_to_apply: list[str] = []
    patch_hashes: dict[str, str] = {}

    # pylint: disable=import-outside-toplevel
    from urllib.request import urlopen

    index_name = remote + '.diff/Index'

    re_whitespace = re.compile(r'\s+')

    try:
        with urlopen(index_name) as index_url:
            index_fields = list(PackageFile(index_name, index_url))
    except ParseError:
        # FIXME: urllib does not raise a proper exception, so we parse
        # the error message.
        if verbose:
            print("update_file: could not interpret patch index file")
        return download_file(remote, local)
    except OSError:
        if verbose:
            print("update_file: could not download patch index file")
        return download_file(remote, local)

    if 'SHA256-Current' in [k for fields in index_fields for k,v in fields]:
        if verbose:
            print("update_file: using sha256")
        prefix = 'SHA256'
        local_hash = read_lines_sha256(lines)
        read_lines = read_lines_sha256
    else:
        if verbose:
            print("update_file: using sha1")
        prefix = 'SHA1'
        local_hash = read_lines_sha1(lines)
        read_lines = read_lines_sha1

    for fields in index_fields:
        for (field, value) in fields:
            if field == prefix+'-Current':
                (remote_hash, _) = re_whitespace.split(value)
                if local_hash == remote_hash:
                    if verbose:
                        print("update_file: local file is up-to-date")
                    return lines
                continue

            if field == prefix+'-History':
                for entry in value.splitlines():
                    if entry == '':
                        continue
                    (hist_hash, _, patch_name) = re_whitespace.split(entry)

                    # After the first patch, we have to apply all
                    # remaining patches.
                    if patches_to_apply or hist_hash == local_hash:
                        patches_to_apply.append(patch_name)

                continue

            if field == prefix+'-Patches':
                for entry in value.splitlines():
                    if entry == '':
                        continue
                    (patch_hash, _, patch_name) = re_whitespace.split(entry)
                    patch_hashes[patch_name] = patch_hash
                continue

            if verbose:
                print("update_file: field %r ignored" % field)

    if not patches_to_apply:
        if verbose:
            print("update_file: could not find historic entry", local_hash)
        return download_file(remote, local)

    for patch_name in patches_to_apply:
        if verbose:
            print("update_file: downloading patch %r" % patch_name)
        patch_contents = download_gunzip_lines(
            remote + '.diff/' + patch_name + '.gz')
        if read_lines(patch_contents) != patch_hashes[patch_name]:
            raise ValueError("patch %r was garbled" % patch_name)
        patch_contents_unicode = list(patch_contents)
        patch_lines(lines, patches_from_ed_script(patch_contents_unicode))

    new_hash = read_lines(lines)
    if new_hash != remote_hash:
        raise ValueError("patch failed, got %s instead of %s"
                         % (new_hash, remote_hash))

    replace_file(lines, local)
    return lines


def merge_as_sets(*args):           # type: ignore
    """Create an order set (represented as a list) of the objects in
    the sequences passed as arguments."""
    s = {}
    for x in args:
        for y in x:
            s[y] = True
    return sorted(s)
