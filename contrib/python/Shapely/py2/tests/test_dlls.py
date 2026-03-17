import sys
import os

from . import unittest
from shapely.geos import load_dll


class LoadingTestCase(unittest.TestCase):

    def test_load(self):
        self.assertRaises(OSError, load_dll, 'geosh_c')
    @unittest.skipIf(sys.platform == "win32", "FIXME: adapt test for win32")
    def test_fallbacks(self):
        load_dll('geos_c', fallbacks=[
            os.path.join(sys.prefix, "lib", "libgeos_c.dylib"), # anaconda (Mac OS X)
            '/opt/local/lib/libgeos_c.dylib',  # MacPorts
            '/usr/local/lib/libgeos_c.dylib',  # homebrew (Mac OS X)
            os.path.join(sys.prefix, "lib", "libgeos_c.so"), # anaconda (Linux)
            'libgeos_c.so.1',
            'libgeos_c.so'])


def _test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(LoadingTestCase)
