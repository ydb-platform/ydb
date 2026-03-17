from uuid import uuid4
from contextlib import contextmanager

TEMPFILES_PATH = '/tempfiles/'

class TemporaryFileProvider(object):
    def __init__(self):
        self.files = {}
        self.prefix = TEMPFILES_PATH
    
    def set_prefix(self, prefix):
        self.prefix = prefix
    
    def create_url(self, filename):
        subpath = self.prefix + str(uuid4())
        self.files[subpath] = filename
        return subpath
    
    def delete_url(self, subpath):
        del self.files[subpath]
    
    @contextmanager
    def temp_file_url(self, filename):
        try:
            subpath = self.create_url(filename)
            yield subpath
        finally:
            self.delete_url(subpath)