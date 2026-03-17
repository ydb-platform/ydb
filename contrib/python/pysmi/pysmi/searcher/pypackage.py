#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import importlib.machinery
import importlib.util
import os
import struct
import time

from pysmi import debug, error
from pysmi.compat import decode
from pysmi.searcher.base import AbstractSearcher
from pysmi.searcher.pyfile import PyFileSearcher

PY_MAGIC_NUMBER = importlib.util.MAGIC_NUMBER
SOURCE_SUFFIXES = importlib.machinery.SOURCE_SUFFIXES
BYTECODE_SUFFIXES = importlib.machinery.BYTECODE_SUFFIXES


class PyPackageSearcher(AbstractSearcher):
    """Figures out if given Python module (source or bytecode) exists in given Python package.

    Python package must be importable.
    """

    def __init__(self, package):
        """Create an instance of *PyPackageSearcher* bound to specific Python package.

        Args:
            package (str): name of the Python package to look up Python
                           modules at.
        """
        self._package = package
        self.__loader = None

    def __str__(self):
        """Return a string representation of the instance."""
        return f'{self.__class__.__name__}{{"{self._package}"}}'

    @staticmethod
    def _parse_dos_time(dosdate, dostime):
        t = (
            ((dosdate >> 9) & 0x7F) + 1980,  # year
            ((dosdate >> 5) & 0x0F),  # month
            dosdate & 0x1F,  # mday
            (dostime >> 11) & 0x1F,  # hour
            (dostime >> 5) & 0x3F,  # min
            (dostime & 0x1F) * 2,  # sec
            -1,  # wday
            -1,  # yday
            -1,
        )  # dst
        return time.mktime(t)

    def file_exists(self, mibname, mtime, rebuild=False):
        if rebuild:
            debug.logger & debug.FLAG_SEARCHER and debug.logger(
                f"pretend {mibname} is very old"
            )
            return

        mibname = decode(mibname)

        try:
            p = __import__(self._package, globals(), locals(), ["__init__"])

            if hasattr(p, "__loader__") and hasattr(p.__loader__, "_files"):
                self.__loader = p.__loader__
                self._package = self._package.replace(".", os.sep)
                debug.logger & debug.FLAG_SEARCHER and debug.logger(
                    f"{self._package} is an importable egg at {os.path.split(p.__file__)[0]}"
                )

            elif hasattr(p, "__file__"):
                debug.logger & debug.FLAG_SEARCHER and debug.logger(
                    f"{self._package} is not an egg, trying it as a package directory"
                )
                return PyFileSearcher(os.path.split(p.__file__)[0]).file_exists(
                    mibname, mtime, rebuild=rebuild
                )

            else:
                raise error.PySmiFileNotFoundError(
                    f"{self._package} is neither importable nor a file", searcher=self
                )

        except ImportError:
            raise error.PySmiFileNotFoundError(
                f"{self._package} is not importable, trying as a path", searcher=self
            )

        for pySfx in BYTECODE_SUFFIXES:
            f = os.path.join(self._package, mibname.upper()) + pySfx

            if f not in self.__loader._files:
                debug.logger & debug.FLAG_SEARCHER and debug.logger(
                    f"{f} is not in {self._package}"
                )
                continue

            pyData = self.__loader.get_data(f)
            if pyData[:4] == PY_MAGIC_NUMBER:
                pyData = pyData[4:]
                pyTime = struct.unpack("<L", pyData[:4])[0]
                debug.logger & debug.FLAG_SEARCHER and debug.logger(
                    f"found {f}, mtime {time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(pyTime))}"
                )
                if pyTime >= mtime:
                    raise error.PySmiFileNotModifiedError()
                else:
                    raise error.PySmiFileNotFoundError(
                        f"older file {mibname} exists", searcher=self
                    )

            else:
                debug.logger & debug.FLAG_SEARCHER and debug.logger(f"bad magic in {f}")
                continue

        for pySfx in SOURCE_SUFFIXES:
            f = os.path.join(self._package, mibname.upper()) + pySfx

            if f not in self.__loader._files:
                debug.logger & debug.FLAG_SEARCHER and debug.logger(
                    f"{f} is not in {self._package}"
                )
                continue

            pyTime = self._parse_dos_time(
                self.__loader._files[f][6], self.__loader._files[f][5]
            )

            debug.logger & debug.FLAG_SEARCHER and debug.logger(
                f"found {f}, mtime {time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(pyTime))}"
            )
            if pyTime >= mtime:
                raise error.PySmiFileNotModifiedError()
            else:
                raise error.PySmiFileNotFoundError(
                    f"older file {mibname} exists", searcher=self
                )

        raise error.PySmiFileNotFoundError(f"no file {mibname} found", searcher=self)
