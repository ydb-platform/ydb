# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import tempfile


class FileResource(object):
    """
    Object that "owns" a file on a filesystem. If the ``filename`` is None,
    it maintains a temporary file which name is accessible via ``name``
    attribute; when pickling, the contents of this file is pickled;
    when unpickling, a new temp file is created; temp files are auto-deleted.
    """
    def __init__(self, filename=None, keep_tempfiles=False, suffix='', prefix=''):
        self.name = filename
        self.auto = filename is None
        self.keep_tempfiles = keep_tempfiles
        self.suffix = suffix
        self.prefix = prefix

    def ensure_name(self):
        """ Ensure that a filename is available """
        if self.name is not None:
            return
        if self.auto:
            fd, self.name = tempfile.mkstemp(self.suffix, self.prefix)
        else:
            raise ValueError("File name is not provided")

    def cleanup(self):
        """ Clean temporary files if needed """
        if self.keep_tempfiles or not self.auto:
            return

        if self.name is not None:
            try:
                os.unlink(self.name)
            except OSError:
                pass
            self.name = None

    def refresh(self):
        self.cleanup()
        self.ensure_name()

    def __del__(self):
        self.cleanup()

    def __getstate__(self):
        dct = self.__dict__.copy()

        if self.auto:
            filename = dct['name']
            if filename is not None:
                try:
                    with open(filename, 'rb') as f:
                        dct['__FILE_RESOURCE_DATA__'] = f.read()
                except IOError:
                    pass
                dct['name'] = None

        return dct

    def __setstate__(self, state):
        data = state.pop('__FILE_RESOURCE_DATA__', None)
        self.__dict__.update(state)

        if data is not None:
            assert self.name is None
            self.ensure_name()
            with open(self.name, 'wb') as f:
                f.write(data)

