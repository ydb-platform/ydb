from __future__ import annotations

import os
import shutil
import tempfile
from typing import Any, Optional

pjoin = os.path.join

from .nbexamples import nb0


def open_utf8(fname, mode):
    return open(fname, mode=mode, encoding="utf-8")  # noqa


class NBFormatTest:
    """Mixin for writing notebook format tests"""

    # override with appropriate values in subclasses
    nb0_ref: Optional[Any] = None
    ext: Optional[str] = None
    mod: Optional[Any] = None

    def setUp(self):
        self.wd = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.wd)

    def assertNBEquals(self, nba, nbb):
        self.assertEqual(nba, nbb)  # type:ignore[attr-defined]

    def test_writes(self):
        assert self.mod is not None
        s = self.mod.writes(nb0)
        if self.nb0_ref:
            self.assertNBEquals(s, self.nb0_ref)

    def test_reads(self):
        assert self.mod is not None
        s = self.mod.writes(nb0)
        nb = self.mod.reads(s)

    def test_roundtrip(self):
        assert self.mod is not None
        s = self.mod.writes(nb0)
        self.assertNBEquals(self.mod.reads(s), nb0)

    def test_write_file(self):
        assert self.mod is not None
        with open_utf8(pjoin(self.wd, "nb0.%s" % self.ext), "w") as f:
            self.mod.write(nb0, f)

    def test_read_file(self):
        assert self.mod is not None
        with open_utf8(pjoin(self.wd, "nb0.%s" % self.ext), "w") as f:
            self.mod.write(nb0, f)

        with open_utf8(pjoin(self.wd, "nb0.%s" % self.ext), "r") as f:
            nb = self.mod.read(f)
