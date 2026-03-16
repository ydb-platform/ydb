from __future__ import annotations

from unittest import TestCase

from nbformat.v1.nbjson import reads, writes

from .nbexamples import nb0


class TestJSON(TestCase):
    def test_roundtrip(self):
        s = writes(nb0)
        self.assertEqual(reads(s), nb0)
