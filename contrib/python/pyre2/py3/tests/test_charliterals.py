import re2 as re
import warnings

warnings.filterwarnings('ignore', category=DeprecationWarning)

import unittest

class TestCharLiterals(unittest.TestCase):
    def test_character_literals(self):
        i = 126

        assert re.compile(r"\%03o" % i) == re.compile('\\176')
        assert re.compile(r"\%03o" % i)._dump_pattern() == '\\176'
        assert (re.match(r"\%03o" % i, chr(i)) is None) == False
        assert (re.match(r"\%03o0" % i, chr(i) + "0") is None) == False
        assert (re.match(r"\%03o8" % i, chr(i) + "8") is None) == False
        assert (re.match(r"\x%02x" % i, chr(i)) is None) == False
        assert (re.match(r"\x%02x0" % i, chr(i) + "0") is None) == False
        assert (re.match(r"\x%02xz" % i, chr(i) + "z") is None) == False

        try:
            re.match("\911", "")
        except Exception as exp:
            assert exp.msg == "invalid group reference 91 at position 1"


    def test_character_class_literals(self):
        i = 126

        assert (re.match(r"[\%03o]" % i, chr(i)) is None) == False
        assert (re.match(r"[\%03o0]" % i, chr(i) + "0") is None) == False
        assert (re.match(r"[\%03o8]" % i, chr(i) + "8") is None) == False
        assert (re.match(r"[\x%02x]" % i, chr(i)) is None) == False
        assert (re.match(r"[\x%02x0]" % i, chr(i) + "0") is None) == False
        assert (re.match(r"[\x%02xz]" % i, chr(i) + "z") is None) == False

        try:
            re.match("[\911]", "")
        except Exception as exp:
            assert exp.msg == "invalid escape sequence: \9"
