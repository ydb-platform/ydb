import contextlib
import tempfile
import shutil

@contextlib.contextmanager
def create_temporary_dir():
    dir = tempfile.mkdtemp()
    try:
        yield dir
    finally:
        shutil.rmtree(dir)
