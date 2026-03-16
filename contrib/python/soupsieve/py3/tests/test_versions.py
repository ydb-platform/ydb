"""Version tests."""
import unittest
from soupsieve.__meta__ import Version, parse_version


class TestVersion(unittest.TestCase):
    """Test versions."""

    def test_version_output(self):
        """Test that versions generate proper strings."""

        assert Version(1, 0, 0, "final")._get_canonical() == "1.0"
        assert Version(1, 2, 0, "final")._get_canonical() == "1.2"
        assert Version(1, 2, 3, "final")._get_canonical() == "1.2.3"
        assert Version(1, 2, 0, "alpha", pre=4)._get_canonical() == "1.2a4"
        assert Version(1, 2, 0, "beta", pre=4)._get_canonical() == "1.2b4"
        assert Version(1, 2, 0, "candidate", pre=4)._get_canonical() == "1.2rc4"
        assert Version(1, 2, 0, "final", post=1)._get_canonical() == "1.2.post1"
        assert Version(1, 2, 3, ".dev-alpha", pre=1)._get_canonical() == "1.2.3a1.dev0"
        assert Version(1, 2, 3, ".dev")._get_canonical() == "1.2.3.dev0"
        assert Version(1, 2, 3, ".dev", dev=1)._get_canonical() == "1.2.3.dev1"

    def test_version_comparison(self):
        """Test that versions compare proper."""

        assert Version(1, 0, 0, "final") < Version(1, 2, 0, "final")
        assert Version(1, 2, 0, "alpha", pre=4) < Version(1, 2, 0, "final")
        assert Version(1, 2, 0, "final") < Version(1, 2, 0, "final", post=1)
        assert Version(1, 2, 3, ".dev-beta", pre=2) < Version(1, 2, 3, "beta", pre=2)
        assert Version(1, 2, 3, ".dev") < Version(1, 2, 3, ".dev-beta", pre=2)
        assert Version(1, 2, 3, ".dev") < Version(1, 2, 3, ".dev", dev=1)

    def test_version_parsing(self):
        """Test version parsing."""

        assert parse_version(
            Version(1, 0, 0, "final")._get_canonical()
        ) == Version(1, 0, 0, "final")
        assert parse_version(
            Version(1, 2, 0, "final")._get_canonical()
        ) == Version(1, 2, 0, "final")
        assert parse_version(
            Version(1, 2, 3, "final")._get_canonical()
        ) == Version(1, 2, 3, "final")
        assert parse_version(
            Version(1, 2, 0, "alpha", pre=4)._get_canonical()
        ) == Version(1, 2, 0, "alpha", pre=4)
        assert parse_version(
            Version(1, 2, 0, "beta", pre=4)._get_canonical()
        ) == Version(1, 2, 0, "beta", pre=4)
        assert parse_version(
            Version(1, 2, 0, "candidate", pre=4)._get_canonical()
        ) == Version(1, 2, 0, "candidate", pre=4)
        assert parse_version(
            Version(1, 2, 0, "final", post=1)._get_canonical()
        ) == Version(1, 2, 0, "final", post=1)
        assert parse_version(
            Version(1, 2, 3, ".dev-alpha", pre=1)._get_canonical()
        ) == Version(1, 2, 3, ".dev-alpha", pre=1)
        assert parse_version(
            Version(1, 2, 3, ".dev")._get_canonical()
        ) == Version(1, 2, 3, ".dev")
        assert parse_version(
            Version(1, 2, 3, ".dev", dev=1)._get_canonical()
        ) == Version(1, 2, 3, ".dev", dev=1)

    def test_asserts(self):
        """Test asserts."""

        with self.assertRaises(ValueError):
            Version("1", "2", "3")
        with self.assertRaises(ValueError):
            Version(1, 2, 3, 1)
        with self.assertRaises(ValueError):
            Version("1", "2", "3")
        with self.assertRaises(ValueError):
            Version(1, 2, 3, "bad")
        with self.assertRaises(ValueError):
            Version(1, 2, 3, "alpha")
        with self.assertRaises(ValueError):
            Version(1, 2, 3, "alpha", pre=1, dev=1)
        with self.assertRaises(ValueError):
            Version(1, 2, 3, "alpha", pre=1, post=1)
        with self.assertRaises(ValueError):
            Version(1, 2, 3, ".dev-alpha")
        with self.assertRaises(ValueError):
            Version(1, 2, 3, ".dev-alpha", pre=1, post=1)
        with self.assertRaises(ValueError):
            Version(1, 2, 3, pre=1)
        with self.assertRaises(ValueError):
            Version(1, 2, 3, dev=1)
        with self.assertRaises(ValueError):
            parse_version('bad&version')
