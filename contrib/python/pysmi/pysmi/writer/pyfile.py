#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import importlib.machinery
import os
import py_compile
import sys
import tempfile

from pysmi import debug
from pysmi import error
from pysmi.compat import decode, encode
from pysmi.writer.base import AbstractWriter

SOURCE_SUFFIXES = importlib.machinery.SOURCE_SUFFIXES


class PyFileWriter(AbstractWriter):
    """Stores transformed MIB modules as Python files at specified location.

    User is expected to pass *PyFileWriter* class instance to
    *MibCompiler* on instantiation. The rest is internal to *MibCompiler*.
    """

    pyCompile = True
    pyOptimizationLevel = -1

    def __init__(self, path):
        """Creates an instance of *PyFileWriter* class.

        Args:
            path: writable directory to store Python modules
        """
        self._path = decode(os.path.normpath(path))

    def __str__(self):
        """Return a string representation of the instance."""
        return f'{self.__class__.__name__}{{"{self._path}"}}'

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

        pyfile = os.path.join(self._path, decode(mibname))
        pyfile += SOURCE_SUFFIXES[0]

        tfile = None

        try:
            fd, tfile = tempfile.mkstemp(dir=self._path)
            os.write(fd, encode(data))
            os.close(fd)
            os.rename(tfile, pyfile)

        except (OSError, UnicodeEncodeError):
            exc = sys.exc_info()
            if tfile and os.access(tfile, os.F_OK):
                os.unlink(tfile)

            raise error.PySmiWriterError(
                f"failure writing file {pyfile}: {exc[1]}", file=pyfile, writer=self
            )

        debug.logger & debug.FLAG_WRITER and debug.logger(f"created file {pyfile}")

        if self.pyCompile:
            try:
                py_compile.compile(
                    pyfile, doraise=True, optimize=self.pyOptimizationLevel
                )

            except (SyntaxError, py_compile.PyCompileError):
                pass  # XXX

            except Exception:
                if pyfile and os.access(pyfile, os.F_OK):
                    os.unlink(pyfile)

                raise error.PySmiWriterError(
                    f"failure compiling {pyfile}: {sys.exc_info()[1]}",
                    file=mibname,
                    writer=self,
                )

        debug.logger & debug.FLAG_WRITER and debug.logger(f"{mibname} stored")

    def get_data(self, filename):
        return ""
