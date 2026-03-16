import unittest
from markdown.__meta__ import _get_version, __version__


class TestVersion(unittest.TestCase):

    def test_get_version(self):
        """Test that _get_version formats __version_info__ as required by PEP 440."""

        self.assertEqual(_get_version((1, 1, 2, 'dev', 0)), "1.1.2.dev0")
        self.assertEqual(_get_version((1, 1, 2, 'alpha', 1)), "1.1.2a1")
        self.assertEqual(_get_version((1, 2, 0, 'beta', 2)), "1.2b2")
        self.assertEqual(_get_version((1, 2, 0, 'rc', 4)), "1.2rc4")
        self.assertEqual(_get_version((1, 2, 0, 'final', 0)), "1.2")

    def test__version__IsValid(self):
        """Test that __version__ is valid and normalized."""

        try:
            import packaging.version
        except ImportError:
            self.skipTest('packaging does not appear to be installed')

        self.assertEqual(__version__, str(packaging.version.Version(__version__)))
