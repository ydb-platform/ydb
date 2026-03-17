import tempfile
from pathlib import Path
from contextlib import contextmanager
import shutil


@contextmanager
def make_tempdir(files={}, mode="w"):
    temp_dir_str = tempfile.mkdtemp()
    temp_dir = Path(temp_dir_str)
    for name, content in files.items():
        path = temp_dir / name
        with path.open(mode) as file_:
            file_.write(content)
    yield temp_dir
    shutil.rmtree(temp_dir_str)
