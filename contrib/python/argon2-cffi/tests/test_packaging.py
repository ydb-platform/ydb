# SPDX-License-Identifier: MIT


from importlib import metadata

import pytest

import argon2


class TestLegacyMetadataHack:
    def test_version(self):
        """
        argon2.__version__ returns the correct version.
        """
        with pytest.deprecated_call():
            assert metadata.version("argon2-cffi") == argon2.__version__

    def test_description(self):
        """
        argon2.__description__ returns the correct description.
        """
        with pytest.deprecated_call():
            assert "Argon2 for Python" == argon2.__description__

    def test_uri(self):
        """
        argon2.__uri__ returns the correct project URL.
        """
        with pytest.deprecated_call():
            assert "https://argon2-cffi.readthedocs.io/" == argon2.__uri__

        with pytest.deprecated_call():
            assert "https://argon2-cffi.readthedocs.io/" == argon2.__url__

    def test_email(self):
        """
        argon2.__email__ returns Hynek's email address.
        """
        with pytest.deprecated_call():
            assert "hs@ox.cx" == argon2.__email__

    def test_does_not_exist(self):
        """
        Asking for unsupported dunders raises an AttributeError.
        """
        with pytest.raises(
            AttributeError, match="module argon2 has no attribute __yolo__"
        ):
            argon2.__yolo__  # noqa: B018
