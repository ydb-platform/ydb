#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#


class AbstractParser:
    def reset(self):
        raise NotImplementedError()

    def parse(self, data, **kwargs):
        raise NotImplementedError()
