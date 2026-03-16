"""
Contains base test class for nbformat
"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import os
import unittest


class TestsBase(unittest.TestCase):
    """Base tests class."""

    @classmethod
    def fopen(cls, f, mode="r", encoding="utf-8"):
        return open(os.path.join(cls._get_files_path(), f), mode, encoding=encoding)  # noqa

    @classmethod
    def _get_files_path(cls):
        import yatest.common as yc
        return yc.source_path(os.path.dirname(__file__))
