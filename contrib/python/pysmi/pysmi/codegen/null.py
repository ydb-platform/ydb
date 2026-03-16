#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
from pysmi import debug
from pysmi.codegen.base import AbstractCodeGen
from pysmi.mibinfo import MibInfo


class NullCodeGen(AbstractCodeGen):
    """Dummy code generation backend.

    Could be used for disabling code generation at *MibCompiler*.
    """

    def gen_code(self, ast, symbolTable, **kwargs):
        debug.logger & debug.FLAG_CODEGEN and debug.logger(
            f"{self.__class__.__name__} invoked"
        )
        return MibInfo(oid=None, name="", imported=[]), ""

    def gen_index(self, mibsMap, **kwargs):
        return ""
