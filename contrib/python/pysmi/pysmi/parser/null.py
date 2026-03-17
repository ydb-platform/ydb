#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
from pysmi.parser.base import AbstractParser


class NullParser(AbstractParser):
    def __init__(self, startSym="mibFile", tempdir=""):
        pass

    def reset(self):
        pass

    def parse(self, data, **kwargs):
        return []
