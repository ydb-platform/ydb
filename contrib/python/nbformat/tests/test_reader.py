"""
Contains tests class for reader.py
"""
# -----------------------------------------------------------------------------
#  Copyright (C) 2013  The IPython Development Team
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE, distributed as part of this software.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from __future__ import annotations

from nbformat.reader import get_version, read
from nbformat.validator import ValidationError

from .base import TestsBase

# -----------------------------------------------------------------------------
# Classes and functions
# -----------------------------------------------------------------------------


class TestReader(TestsBase):
    def test_read(self):
        """Can older notebooks be opened without modification?"""

        # Open a version 3 notebook.  Make sure it is still version 3.
        with self.fopen("test3.ipynb", "r") as f:
            nb = read(f)
        (major, minor) = get_version(nb)
        self.assertEqual(major, 3)

        # Open a version 2 notebook.  Make sure it is still version 2.
        with self.fopen("test2.ipynb", "r") as f:
            nb = read(f)
        (major, minor) = get_version(nb)
        self.assertEqual(major, 2)

    def test_read_fails_on_missing_worksheets(self):
        with self.fopen("test3_no_worksheets.ipynb", "r") as f, self.assertRaisesRegex(
            ValidationError, r"worksheets"
        ):
            nb = read(f)

    def test_read_fails_on_missing_worksheet_cells(self):
        with self.fopen("test3_worksheet_with_no_cells.ipynb", "r") as f, self.assertRaisesRegex(
            ValidationError, r"cells"
        ):
            nb = read(f)
