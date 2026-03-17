from __future__ import annotations

from unittest import TestCase

from nbformat.v2.nbpy import writes

from .nbexamples import nb0, nb0_py


class TestPy(TestCase):
    def test_write(self):
        s = writes(nb0)
        self.assertEqual(s, nb0_py)
