import shutil
import tempfile
import unittest
from pathlib import Path


class QueuelibTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="queuelib-tests-")
        self.qpath: Path = self.tempfilename()
        self.qdir = self.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.qdir)
        shutil.rmtree(self.tmpdir)

    def tempfilename(self) -> Path:
        with tempfile.NamedTemporaryFile(dir=self.tmpdir) as nf:
            return Path(nf.name)

    def mkdtemp(self) -> str:
        return tempfile.mkdtemp(dir=self.tmpdir)


def track_closed(cls):
    """Wraps a queue class to track down if close() method was called"""

    class TrackingClosed(cls):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.closed = False

        def close(self):
            super().close()
            self.closed = True

    return TrackingClosed
