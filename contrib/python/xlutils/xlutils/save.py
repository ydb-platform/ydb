# Copyright (c) 2008-2012 Simplistix Ltd
#
# This Software is released under the MIT License:
# http://www.opensource.org/licenses/mit-license.html
# See license.txt for more details.

import os
from xlutils.filter import process,XLRDReader,StreamWriter
from .compat import basestring


def save(wb, filename_or_stream):
    "Save the supplied :class:`xlrd.Book` to the supplied stream or filename."
    if isinstance(filename_or_stream,basestring):
        filename = os.path.split(filename_or_stream)[1]
        stream = open(filename_or_stream,'wb')
        close = True
    else:
        filename = 'unknown.xls'
        stream = filename_or_stream
        close = False
    process(
        XLRDReader(wb,filename),
        StreamWriter(stream)
        )
    if close:
        stream.close()
