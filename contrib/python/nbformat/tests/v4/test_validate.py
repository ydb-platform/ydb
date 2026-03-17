"""Tests for nbformat validation"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import os

import pytest

from nbformat.v4.nbbase import nbformat, new_code_cell, new_markdown_cell, new_raw_cell
from nbformat.v4.nbjson import reads
from nbformat.validator import ValidationError, validate


def validate4(obj, ref=None):
    return validate(obj, ref, version=nbformat)


def test_valid_code_cell():
    cell = new_code_cell()
    validate4(cell, "code_cell")


def test_invalid_code_cell():
    cell = new_code_cell()

    cell["source"] = 5
    with pytest.raises(ValidationError):
        validate4(cell, "code_cell")

    cell = new_code_cell()
    del cell["metadata"]

    with pytest.raises(ValidationError):
        validate4(cell, "code_cell")

    cell = new_code_cell()
    del cell["source"]

    with pytest.raises(ValidationError):
        validate4(cell, "code_cell")

    cell = new_code_cell()
    del cell["cell_type"]

    with pytest.raises(ValidationError):
        validate4(cell, "code_cell")


def test_invalid_markdown_cell():
    cell = new_markdown_cell()

    cell["source"] = 5
    with pytest.raises(ValidationError):
        validate4(cell, "markdown_cell")

    cell = new_markdown_cell()
    del cell["metadata"]

    with pytest.raises(ValidationError):
        validate4(cell, "markdown_cell")

    cell = new_markdown_cell()
    del cell["source"]

    with pytest.raises(ValidationError):
        validate4(cell, "markdown_cell")

    cell = new_markdown_cell()
    del cell["cell_type"]

    with pytest.raises(ValidationError):
        validate4(cell, "markdown_cell")


def test_invalid_raw_cell():
    cell = new_raw_cell()

    cell["source"] = 5
    with pytest.raises(ValidationError):
        validate4(cell, "raw_cell")

    cell = new_raw_cell()
    del cell["metadata"]

    with pytest.raises(ValidationError):
        validate4(cell, "raw_cell")

    cell = new_raw_cell()
    del cell["source"]

    with pytest.raises(ValidationError):
        validate4(cell, "raw_cell")

    cell = new_raw_cell()
    del cell["cell_type"]

    with pytest.raises(ValidationError):
        validate4(cell, "raw_cell")


def test_sample_notebook():
    import yatest.common as yc
    here = yc.source_path(os.path.dirname(__file__))
    with open(
        os.path.join(here, os.pardir, os.pardir, "tests", "test4.ipynb"),
        encoding="utf-8",
    ) as f:
        nb = reads(f.read())
    validate4(nb)
