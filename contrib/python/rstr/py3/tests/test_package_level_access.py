import unittest
import re

import rstr


class TestPackageLevelFunctions(unittest.TestCase):

    def test_rstr(self) -> None:
        assert re.match(r'^[ABC]+$', rstr.rstr('ABC'))

    def test_xeger(self) -> None:
        assert re.match(r'^foo[\d]{10}bar$', rstr.xeger(r'^foo[\d]{10}bar$'))

    def test_convenience_function(self) -> None:
        assert re.match(r'^[a-zA-Z]+$', rstr.letters())
