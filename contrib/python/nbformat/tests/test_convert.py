"""Tests for nbformat.convert"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

from nbformat import current_nbformat
from nbformat.converter import convert
from nbformat.reader import get_version, read
from nbformat.validator import ValidationError, isvalid, validate

from .base import TestsBase


class TestConvert(TestsBase):
    def test_downgrade_3_2(self):
        """Do notebook downgrades work?"""

        # Open a version 3 notebook and attempt to downgrade it to version 2.
        with self.fopen("test3.ipynb", "r") as f:
            nb = read(f)
        nb = convert(nb, 2)

        # Check if downgrade was successful.
        (major, minor) = get_version(nb)
        self.assertEqual(major, 2)

    def test_upgrade_2_3(self):
        """Do notebook upgrades work?"""

        # Open a version 2 notebook and attempt to upgrade it to version 3.
        with self.fopen("test2.ipynb", "r") as f:
            nb = read(f)
        nb = convert(nb, 3)

        # Check if upgrade was successful.
        (major, minor) = get_version(nb)
        self.assertEqual(major, 3)

    def test_upgrade_downgrade_4_3_4(self):
        """Test that a v4 notebook downgraded to v3 and then upgraded to v4
        passes validation tests"""
        with self.fopen("test4.ipynb", "r") as f:
            nb = read(f)
        validate(nb)
        nb = convert(nb, 3)
        validate(nb)
        nb = convert(nb, 4)
        self.assertEqual(isvalid(nb), True)

    def test_upgrade_3_4__missing_metadata(self):
        with self.fopen("test3_no_metadata.ipynb", "r") as f:
            nb = read(f)

        with self.assertRaisesRegex(ValidationError, r"could not be converted.+metadata"):
            convert(nb, 4)

    def test_open_current(self):
        """Can an old notebook be opened and converted to the current version
        while remembering the original version of the notebook?"""

        # Open a version 2 notebook and attempt to upgrade it to the current version
        # while remembering it's version information.
        with self.fopen("test2.ipynb", "r") as f:
            nb = read(f)
        (original_major, original_minor) = get_version(nb)
        nb = convert(nb, current_nbformat)

        # Check if upgrade was successful.
        (major, minor) = get_version(nb)
        self.assertEqual(major, current_nbformat)

        # Check if the original major revision was remembered.
        self.assertEqual(original_major, 2)
