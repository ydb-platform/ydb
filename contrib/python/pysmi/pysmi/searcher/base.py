#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#


class AbstractSearcher:
    def set_options(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
        return self

    def file_exists(self, mibname, mtime, rebuild=False):
        raise NotImplementedError()
