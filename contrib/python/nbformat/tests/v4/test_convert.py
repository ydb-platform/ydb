from __future__ import annotations

import copy
import os
from unittest import mock

import pytest

from nbformat import ValidationError, v3, v4, validate
from nbformat.v4 import convert
from nbformat.v4.nbjson import reads

from ..v3 import nbexamples as v3examples
from . import nbexamples

import yatest.common as yc


def test_upgrade_notebook():
    nb03 = copy.deepcopy(v3examples.nb0)
    validate(nb03)
    nb04 = convert.upgrade(nb03)
    validate(nb04)


def test_downgrade_notebook():
    nb04 = copy.deepcopy(nbexamples.nb0)
    validate(nb04)
    nb03 = convert.downgrade(nb04)
    validate(nb03)


def test_upgrade_heading():
    # Fake the uuid generation for ids
    cell_ids = ["cell-1", "cell-2", "cell-3"]
    with mock.patch("nbformat.v4.convert.random_cell_id", side_effect=cell_ids), mock.patch(
        "nbformat.v4.nbbase.random_cell_id", side_effect=cell_ids
    ):
        v3h = v3.new_heading_cell
        v4m = v4.new_markdown_cell
        for v3cell, expected in [
            (
                v3h(source="foo", level=1),
                v4m(source="# foo"),
            ),
            (
                v3h(source="foo\nbar\nmulti-line\n", level=4),
                v4m(source="#### foo bar multi-line"),
            ),
            (
                v3h(source="ünìcö∂e–cønvërsioñ", level=4),
                v4m(source="#### ünìcö∂e–cønvërsioñ"),
            ),
        ]:
            upgraded = convert.upgrade_cell(v3cell)
            assert upgraded == expected


def test_downgrade_heading():
    v3h = v3.new_heading_cell
    v4m = v4.new_markdown_cell
    v3m = lambda source: v3.new_text_cell("markdown", source)
    for v4cell, expected in [
        (
            v4m(source="# foo"),
            v3h(source="foo", level=1),
        ),
        (
            v4m(source="#foo"),
            v3h(source="foo", level=1),
        ),
        (
            v4m(source="#\tfoo"),
            v3h(source="foo", level=1),
        ),
        (
            v4m(source="# \t  foo"),
            v3h(source="foo", level=1),
        ),
        (
            v4m(source="# foo\nbar"),
            v3m(source="# foo\nbar"),
        ),
    ]:
        downgraded = convert.downgrade_cell(v4cell)
        assert downgraded == expected


def test_upgrade_v4_to_4_dot_5():
    here = yc.source_path(os.path.dirname(__file__))
    with open(os.path.join(here, os.pardir, "test4.ipynb"), encoding="utf-8") as f:
        nb = reads(f.read())

    assert nb["nbformat_minor"] == 0
    validate(nb)
    assert nb.cells[0].get("id") is None

    nb_up = convert.upgrade(nb)
    assert nb_up["nbformat_minor"] == 5
    validate(nb_up)
    assert nb_up.cells[0]["id"] is not None


def test_upgrade_without_nbminor_version():
    here = yc.source_path(os.path.dirname(__file__))
    with open(os.path.join(here, os.pardir, "no_min_version.ipynb"), encoding="utf-8") as f:
        nb = reads(f.read())

    with pytest.raises(ValidationError):
        convert.upgrade(nb)
