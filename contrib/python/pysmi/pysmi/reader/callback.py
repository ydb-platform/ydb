#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import time

from pysmi import debug, error
from pysmi.mibinfo import MibInfo
from pysmi.reader.base import AbstractReader


class CallbackReader(AbstractReader):
    """Fetch ASN.1 MIB text by name by calling user-defined callable.

    *CallbackReader* class instance tries to retrieve ASN.1 MIB files
    by name and return their contents to caller.
    """

    def __init__(self, cbFun, cbCtx=None):
        """Create an instance of *CallbackReader* bound to specific URL.

        Args:
            cbFun (callable): user callable accepting *MIB name* and *cbCtx* objects

        Keyword Args:
            cbCtx (object): user object that can be used to communicate state information
                between user-scope code and the *cbFun* callable scope
        """
        self._cbFun = cbFun
        self._cbCtx = cbCtx

    def __str__(self):
        """Return string representation of the instance."""
        return f'{self.__class__.__name__}{{"{self._cbFun}"}}'

    def get_data(self, mibname, **options):
        debug.logger & debug.FLAG_READER and debug.logger(
            f"calling user callback {self._cbFun} for MIB {mibname}"
        )

        res = self._cbFun(mibname, self._cbCtx)
        if res:
            return (
                MibInfo(
                    path="file:///dev/stdin", file="", name=mibname, mtime=time.time()
                ),
                res,
            )

        raise error.PySmiReaderFileNotFoundError(mibname=mibname, reader=self)
