#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import os
import sys
import time

from pysmi import debug
from pysmi import error
from pysmi.compat import decode
from pysmi.searcher.base import AbstractSearcher


class AnyFileSearcher(AbstractSearcher):
    """Figures out if given file exists at given location."""

    exts = []

    def __init__(self, path):
        """Create an instance of *AnyFileSearcher* bound to specific directory.

        Args:
          path (str): path to local directory
        """
        self._path = os.path.normpath(decode(path))

    def __str__(self):
        """Return a string representation of the instance."""
        return f'{self.__class__.__name__}{{"{self._path}"}}'

    def file_exists(self, mibname, mtime, rebuild=False):
        if rebuild:
            debug.logger & debug.FLAG_SEARCHER and debug.logger(
                f"pretend {mibname} is very old"
            )
            return

        mibname = decode(mibname)
        basename = os.path.join(self._path, mibname)

        for sfx in self.exts:
            f = basename + sfx
            if not os.path.exists(f) or not os.path.isfile(f):
                debug.logger & debug.FLAG_SEARCHER and debug.logger(
                    f"{f} not present or not a file"
                )
                continue

            try:
                fileTime = os.stat(f)[8]

            except OSError:
                raise error.PySmiSearcherError(
                    f"failure opening compiled file {f}: {sys.exc_info()[1]}",
                    searcher=self,
                )

            debug.logger & debug.FLAG_SEARCHER and debug.logger(
                f"found {f}, mtime {time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(fileTime))}"
            )

            if fileTime >= mtime:
                raise error.PySmiFileNotModifiedError()

        raise error.PySmiFileNotFoundError(
            f"no compiled file {mibname} found", searcher=self
        )
