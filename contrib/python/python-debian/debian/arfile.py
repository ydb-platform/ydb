""" Representation of ar archives for use with Debian binary packages

These classes are primarily intended to be used with the
:class:`debian.debfile.DebFile` class for working with Debian binary
packages.
"""

# Copyright (C) 2007       Stefano Zacchiroli  <zack@debian.org>
# Copyright (C) 2007       Filippo Giunchedi   <filippo@debian.org>
# Copyright (C) 2014-2023  Stuart Prescott     <stuart@debian.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

from pathlib import Path
import sys

from typing import (
    BinaryIO,
    Generator,
    Iterator,
    NoReturn,
)


GLOBAL_HEADER = b"!<arch>\n"
GLOBAL_HEADER_LENGTH = len(GLOBAL_HEADER)

FILE_HEADER_LENGTH = 60
FILE_MAGIC = b"`\n"


class ArError(Exception):
    """ Common base for all exceptions raised within the arfile module """


class ArFile:
    """ Representation of an ar archive, see man 1 ar.

    The interface of this class tries to mimic that of the TarFile module in
    the standard library.

    ArFile objects have the following (read-only) properties:
        - members       same as getmembers()
    """

    def __init__(self,
                 filename: str | Path | None = None,
                 mode: str = 'r',
                 fileobj: BinaryIO | None = None,
                 encoding: str | None = None,
                 errors: str | None = None,
                 ) -> None:
        """ Build an ar file representation starting from either a filename or
        an existing file object. The only supported mode is 'r'.

        The encoding and errors parameters control how member
        names are decoded into Unicode strings. Like tarfile, the default
        encoding is sys.getfilesystemencoding() and the default error handling
        scheme is 'surrogateescape'.
        """
        self.__members: list[ArMember] = []
        self.__members_dict: dict[str, ArMember] = {}
        self.__fname: str | Path | None = filename
        self.__fileobj = fileobj
        self.__encoding = encoding or sys.getfilesystemencoding()
        if errors is None:
            errors = 'surrogateescape'
        self.__errors = errors

        if mode == "r":
            self.__index_archive()
        # TODO write support

    def __index_archive(self) -> None:
        if self.__fname:
            with open(self.__fname, "rb") as fp:
                self.__collect_members(fp)
        elif self.__fileobj:
            self.__collect_members(self.__fileobj)
        else:
            raise ArError("Unable to open valid file")

    def __collect_members(self, fp: BinaryIO) -> None:
        if fp.read(GLOBAL_HEADER_LENGTH) != GLOBAL_HEADER:
            raise ArError("Unable to find global header")

        while True:
            newmember = ArMember.from_file(fp, self.__fname,
                                           encoding=self.__encoding,
                                           errors=self.__errors)
            if not newmember:
                break
            self.__members.append(newmember)
            self.__members_dict[newmember.name] = newmember
            if newmember.size % 2 == 0:   # even, no padding
                fp.seek(newmember.size, 1)   # skip to next header
            else:
                fp.seek(newmember.size + 1, 1)   # skip to next header

    def getmember(self, name: str) -> ArMember:
        """ Return the (last occurrence of a) member in the archive whose name
        is 'name'. Raise KeyError if no member matches the given name.

        Note that in case of name collisions the only way to retrieve all
        members matching a given name is to use getmembers. """

        return self.__members_dict[name]

    def getmembers(self) -> list[ArMember]:
        """ Return a list of all members contained in the archive.

        The list has the same order of members in the archive and can contain
        duplicate members (i.e. members with the same name) if they are
        duplicate in the archive itself. """

        return self.__members

    members = property(getmembers)

    def getnames(self) -> list[str]:
        """ Return a list of all member names in the archive. """

        return [f.name for f in self.__members]

    def extractall(self) -> NoReturn:
        """ Not (yet) implemented. """

        raise NotImplementedError  # TODO

    def extract(self, member: str, path: str) -> NoReturn:
        """ Not (yet) implemented. """

        raise NotImplementedError  # TODO

    def extractfile(self, member: str) -> ArMember | None:
        """ Return a file object corresponding to the requested member. A member
        can be specified either as a string (its name) or as a ArMember
        instance. """

        # TODO(jsw): What is the point of this method?  It differs from
        # getmember in the following ways:
        #  - It returns the *first* member with the given name instead of the
        #    last.
        #  - If member is an ArMember, it uses that ArMember's name as the key.
        # The former just seems confusing (and this implementation less
        # efficient than getmember's - probably historical), and I'm having a
        # hard time seeing the use-case for the latter.
        for m in self.__members:
            if isinstance(member, ArMember) and m.name == member.name:
                return m
            if member == m.name:
                return m
        return None

    # container emulation

    def __iter__(self) -> Iterator[ArMember]:
        """ Iterate over the members of the present ar archive. """

        return iter(self.__members)

    def __getitem__(self, name: str) -> ArMember:
        """ Same as .getmember(name). """

        return self.getmember(name)


class ArMember:
    """ Member of an ar archive.

    Implements most of a file object interface: read, readline, next,
    readlines, seek, tell, close.

    ArMember objects have the following (read-only) properties:
        - name      member name in an ar archive
        - mtime     modification time
        - owner     owner user
        - group     owner group
        - fmode     file permissions
        - size      size in bytes
        - fname     file name"""

    def __init__(self) -> None:
        # member name (i.e. filename) in the archive
        self.__name: str | None = None
        # last modification time
        self.__mtime: int | None = None
        # owner user id
        self.__owner: int | None = None
        # owner group id
        self.__group: int | None = None
        # permissions as octal bytes
        self.__fmode: bytes | None = None
        # member size in bytes
        self.__size: int | None = None
        # file name associated with this member
        self.__fname: str | Path | None = ""
        # file pointer
        self.__fp: BinaryIO | None = None
        # start-of-data offset
        self.__offset: int = 0
        # end-of-data offset
        self.__end: int = 0
        # current position
        self.__cur: int = 0

    @staticmethod
    def from_file(fp: BinaryIO,
                  fname: str | Path | None,
                  encoding: str | None = None,
                  errors: str | None = None,
                  ) -> ArMember | None:
        """fp is an open File object positioned on a valid file header inside
        an ar archive. Return a new ArMember on success, None otherwise. """

        buf = fp.read(FILE_HEADER_LENGTH)

        if not buf:
            return None

        # sanity checks
        if len(buf) < FILE_HEADER_LENGTH:
            raise OSError("Incorrect header length")

        if buf[58:60] != FILE_MAGIC:
            raise OSError("Incorrect file magic")

        if encoding is None:
            encoding = sys.getfilesystemencoding()
        if errors is None:
            errors = 'surrogateescape'

        # http://en.wikipedia.org/wiki/Ar_(Unix)
        # from   to     Name                      Format
        # 0      15     File name                 ASCII
        # 16     27     File modification date    Decimal
        # 28     33     Owner ID                  Decimal
        # 34     39     Group ID                  Decimal
        # 40     47     File mode                 Octal
        # 48     57     File size in bytes        Decimal
        # 58     59     File magic                \140\012

        # XXX struct.unpack can be used as well here
        f = ArMember()

        # pylint: disable=unused-private-member
        # pylint 2.11 is very confused here.
        name = buf[0:16].split(b"/")[0].strip()
        f.__name = name.decode(encoding, errors)
        f.__mtime = int(buf[16:28])
        f.__owner = int(buf[28:34])
        f.__group = int(buf[34:40])
        f.__fmode = buf[40:48]  # XXX octal value
        f.__size = int(buf[48:58])

        f.__fname = fname
        if not fname:
            f.__fp = fp
        f.__offset = fp.tell()   # start-of-data
        f.__end = f.__offset + f.__size
        f.__cur = f.__offset

        return f

    # file interface

    # XXX this is not a sequence like file objects
    def read(self, size: int = 0) -> bytes:
        if self.__fp is None:
            if self.__fname is None:
                raise ValueError("Cannot have both fp and fname undefined")
            self.__fp = open(self.__fname, "rb")  # pylint: disable = consider-using-with
        self.__fp.seek(self.__cur)

        if 0 < size <= self.__end - self.__cur:   # there's room
            buf = self.__fp.read(size)
            self.__cur = self.__fp.tell()
            return buf

        if self.__cur >= self.__end or self.__cur < self.__offset:
            return b''

        buf = self.__fp.read(self.__end - self.__cur)
        self.__cur = self.__fp.tell()
        return buf

    def readline(self, size: int | None = None) -> bytes:
        if self.__fp is None:
            if self.__fname is None:
                raise ValueError("Cannot have both fp and fname undefined")
            self.__fp = open(self.__fname, "rb")  # pylint: disable = consider-using-with
        self.__fp.seek(self.__cur)

        if size is not None:
            buf = self.__fp.readline(size)
            self.__cur = self.__fp.tell()
            if self.__cur > self.__end:
                return b''

            return buf

        buf = self.__fp.readline()
        self.__cur = self.__fp.tell()
        if self.__cur > self.__end:
            return b''
        return buf

    def readlines(self, sizehint: int = 0) -> list[bytes]:
        # pylint: disable=unused-argument
        buf = None
        lines = []
        while True:
            buf = self.readline()
            if not buf:
                break
            lines.append(buf)

        return lines

    def seek(self, offset: int, whence: int = 0) -> None:
        self.__cur = max(self.__cur, self.__offset)

        if whence < 2 and offset + self.__cur < self.__offset:
            raise OSError("Can't seek at %d" % offset)

        if whence == 1:
            self.__cur = self.__cur + offset
        elif whence == 0:
            self.__cur = self.__offset + offset
        elif whence == 2:
            self.__cur = self.__end + offset

    def tell(self) -> int:
        if self.__cur < self.__offset:
            return 0
        return self.__cur - self.__offset

    def seekable(self) -> bool:
        return True

    def close(self) -> None:
        if self.__fp is not None and self.__fname is not None:
            self.__fp.close()
            self.__fp = None

    def next(self) -> bytes:
        return self.readline()

    def __iter__(self) -> Iterator[bytes]:
        def nextline() -> Generator[bytes]:
            line = self.readline()
            if line:
                yield line

        return iter(nextline())

    name = property(lambda self: self.__name)
    mtime = property(lambda self: self.__mtime)
    owner = property(lambda self: self.__owner)
    group = property(lambda self: self.__group)
    fmode = property(lambda self: self.__fmode)
    size = property(lambda self: self.__size)
    fname = property(lambda self: self.__fname)


if __name__ == '__main__':
    # test
    # ar r test.ar <file1> <file2> .. <fileN>
    a = ArFile("test.ar")
    print("\n".join(a.getnames()))
