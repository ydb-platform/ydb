import tempfile
import shutil
import time
import os
from numbers import Number


def create_temp_dir(dir=None):
    path = tempfile.mkdtemp(dir=dir)
    return TemporaryDirectory(path)


class TemporaryDirectory(object):
    def __init__(self, path):
        self.path = path
        
    def close(self):
        shutil.rmtree(self.path)
    
    def __enter__(self):
        return self
        
    def __exit__(self, *args):
        self.close()


def root(dir, timeout=None):
    return Root(path=dir, timeout=timeout)
    
    
class Root(object):
    def __init__(self, path, timeout):
        self._path = path
        if timeout is None or isinstance(timeout, Number):
            self._timeout = timeout
        else:
            self._timeout = self._total_seconds(timeout)
    
    def _total_seconds(self, value):
        if hasattr(value, "total_seconds"):
            return value.total_seconds()
        else:
            return (value.microseconds + (value.seconds + value.days * 24 * 3600) * 10**6) / 10**6
    
    def create_temp_dir(self):
        self.cleanup()
        
        return create_temp_dir(dir=self._path)

    def cleanup(self):
        if self._timeout is not None:
            self._delete_directories_older_than_timeout()
            
    def _delete_directories_older_than_timeout(self):
        now = time.time()
        limit = now - self._timeout

        names = os.listdir(self._path)
        for name in names:
            path = os.path.join(self._path, name)
            stat = os.stat(path)
            
            if max(stat.st_atime, stat.st_mtime) < limit:
                shutil.rmtree(path)
