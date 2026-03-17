#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
from pysmi import debug
from pysmi import error
from pysmi.searcher.base import AbstractSearcher


class StubSearcher(AbstractSearcher):
    """Figures out if given MIB module is present in a fixed list of modules."""

    def __init__(self, *mibnames):
        """Create an instance of *StubSearcher* initialized with a fixed list or MIB modules names.

        Args:
            mibnames (str): blacklisted MIB names
        """
        self._mibnames = mibnames

    def __str__(self):
        """Return a string representation of the instance."""
        return self.__class__.__name__

    def file_exists(self, mibname, mtime, rebuild=False):
        if mibname in self._mibnames:
            debug.logger & debug.FLAG_SEARCHER and debug.logger(
                f"pretend compiled {mibname} exists and is very new"
            )
            raise error.PySmiFileNotModifiedError(
                f"compiled file {mibname} is among {', '.join(self._mibnames)}",
                searcher=self,
            )

        raise error.PySmiFileNotFoundError(
            f"no compiled file {mibname} found among {', '.join(self._mibnames)}",
            searcher=self,
        )
