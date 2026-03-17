# -*- coding: utf8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from io import BytesIO
    from library.python import resource

    def get_file(*args):
        path = '/'.join(('contrib/python/python-docx/docx',) + args)
        return BytesIO(resource.resfs_read(path.encode('utf8')))
except ImportError:
    import os

    base_path = os.path.split(__file__)[0]

    def get_file(*args):
        return open(os.path.join(base_path, *args), 'rb')
