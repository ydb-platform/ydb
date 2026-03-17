import sys
import unittest

from xlrd import biffh

if sys.version_info[0] >= 3:
    from io import StringIO
else:
    # Python 2.6+ does have the io module, but io.StringIO is strict about
    # unicode, which won't work for our test.
    from StringIO import StringIO


class TestHexDump(unittest.TestCase):
    def test_hex_char_dump(self):
        sio = StringIO()
        biffh.hex_char_dump(b"abc\0e\01", 0, 6, fout=sio)
        s = sio.getvalue()
        assert "61 62 63 00 65 01" in s, s
        assert "abc~e?" in s, s

if __name__=='__main__':
    unittest.main()
