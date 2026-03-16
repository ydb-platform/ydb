#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import os
import sys
import tempfile

from pysmi import debug
from pysmi import error
from pysmi.compat import decode, encode
from pysmi.writer.base import AbstractWriter


class FileWriter(AbstractWriter):
    """Stores transformed MIB modules in files at specified location.

    User is expected to pass *FileReader* class instance to
    *MibCompiler* on instantiation. The rest is internal to *MibCompiler*.
    """

    suffix = ""

    def __init__(self, path):
        """Creates an instance of *FileReader* class.

        Args:
            path: writable directory to store created files
        """
        self._path = decode(os.path.normpath(path))

    def __str__(self):
        """Return a string representation of the instance."""
        return f'{self.__class__.__name__}{{"{self._path}"}}'

    def get_data(self, mibname, dryRun=False):
        filename = os.path.join(self._path, decode(mibname)) + self.suffix

        f = None

        try:
            f = open(filename)
            data = f.read()
            f.close()
            return data

        except (OSError, UnicodeEncodeError):
            if f:
                f.close()
            return ""

    def put_data(self, mibname, data, comments=(), dryRun=False):
        if dryRun:
            debug.logger & debug.FLAG_WRITER and debug.logger("dry run mode")
            return

        if not os.path.exists(self._path):
            try:
                os.makedirs(self._path)

            except OSError:
                raise error.PySmiWriterError(
                    f"failure creating destination directory {self._path}: {sys.exc_info()[1]}",
                    writer=self,
                )

        if comments:
            data = f"#{os.linesep}{os.linesep.join([f'# {x}' for x in comments])}{os.linesep}#{os.linesep}{data}"

        filename = os.path.join(self._path, decode(mibname)) + self.suffix

        tfile = None

        try:
            fd, tfile = tempfile.mkstemp(dir=self._path)
            os.write(fd, encode(data))
            os.close(fd)
            os.rename(tfile, filename)

        except (OSError, UnicodeEncodeError):
            exc = sys.exc_info()
            if tfile:
                try:
                    os.unlink(tfile)

                except OSError:
                    pass

            raise error.PySmiWriterError(
                f"failure writing file {filename}: {exc[1]}", file=filename, writer=self
            )

        debug.logger & debug.FLAG_WRITER and debug.logger(
            f"{mibname} stored in {filename}"
        )
