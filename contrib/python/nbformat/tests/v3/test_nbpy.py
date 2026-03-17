from __future__ import annotations

from unittest import TestCase

from nbformat.v3 import nbpy

from . import formattest
from .nbexamples import nb0_py


class TestPy(formattest.NBFormatTest, TestCase):
    nb0_ref = nb0_py
    ext = "py"
    mod = nbpy
    ignored_keys = ["collapsed", "outputs", "prompt_number", "metadata"]  # noqa

    def assertSubset(self, da, db):
        """assert that da is a subset of db, ignoring self.ignored_keys.

        Called recursively on containers, ultimately comparing individual
        elements.
        """
        if isinstance(da, dict):
            for k, v in da.items():
                if k in self.ignored_keys:
                    continue
                self.assertTrue(k in db)
                self.assertSubset(v, db[k])
        elif isinstance(da, list):
            for a, b in zip(da, db):
                self.assertSubset(a, b)
        else:
            if isinstance(da, str) and isinstance(db, str):
                # pyfile is not sensitive to preserving leading/trailing
                # newlines in blocks through roundtrip
                da = da.strip("\n")
                db = db.strip("\n")
            self.assertEqual(da, db)
        return True

    def assertNBEquals(self, nba, nbb):
        # since roundtrip is lossy, only compare keys that are preserved
        # assumes nba is read from my file format
        return self.assertSubset(nba, nbb)
