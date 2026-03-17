# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Iterator, Sequence
from .mtext import MText, MTextColumns

__all__ = [
    "make_static_columns_r2000",
    "make_dynamic_auto_height_columns_r2000",
    "make_dynamic_manual_height_columns_r2000",
    "make_static_columns_r2018",
    "make_dynamic_auto_height_columns_r2018",
    "make_dynamic_manual_height_columns_r2018",
]

COLUMN_BREAK = "\\N"


def add_column_breaks(content: Iterable[str]) -> Iterator[str]:
    content = list(content)
    for c in content[:-1]:
        if not c.endswith(COLUMN_BREAK):
            c += COLUMN_BREAK
        yield c
    yield content[-1]  # last column without a column break


def make_static_columns_r2000(
    content: Sequence[str],
    width: float,
    gutter_width: float,
    height: float,
    dxfattribs=None,
) -> MText:
    if len(content) < 1:
        raise ValueError("no content")
    columns = MTextColumns.new_static_columns(
        len(content), width, gutter_width, height
    )
    mtext = MText.new(dxfattribs=dxfattribs)
    mtext.setup_columns(columns, linked=True)
    content = list(add_column_breaks(content))
    mtext.text = content[0]
    for mt, c in zip(columns.linked_columns, content[1:]):
        mt.text = c
    return mtext


def make_dynamic_auto_height_columns_r2000(
    content: str,
    width: float,
    gutter_width: float,
    height: float,
    count: int = 1,
    dxfattribs=None,
) -> MText:
    if not content:
        raise ValueError("no content")
    mtext = MText.new(dxfattribs=dxfattribs)
    mtext.dxf.width = width
    columns = MTextColumns.new_dynamic_auto_height_columns(
        count, width, gutter_width, height
    )
    set_dynamic_columns_content(content, mtext, columns)
    return mtext


def make_dynamic_manual_height_columns_r2000(
    content: str,
    width: float,
    gutter_width: float,
    heights: Sequence[float],
    dxfattribs=None,
) -> MText:
    if not content:
        raise ValueError("no content")
    mtext = MText.new(dxfattribs=dxfattribs)
    mtext.dxf.width = width
    columns = MTextColumns.new_dynamic_manual_height_columns(
        width, gutter_width, heights
    )
    set_dynamic_columns_content(content, mtext, columns)
    return mtext


def set_dynamic_columns_content(
    content: str, mtext: MText, columns: MTextColumns
):
    mtext.setup_columns(columns, linked=True)
    # temp. hack: assign whole content to the main column
    mtext.text = content
    # for mt, c in zip(mtext.columns.linked_columns, content[1:]):
    #    mt.text = c
    return mtext


# DXF version R2018


def make_static_columns_r2018(
    content: Sequence[str],
    width: float,
    gutter_width: float,
    height: float,
    dxfattribs=None,
) -> MText:
    if len(content) < 1:
        raise ValueError("no content")
    columns = MTextColumns.new_static_columns(
        len(content), width, gutter_width, height
    )
    mtext = MText.new(dxfattribs=dxfattribs)
    mtext.setup_columns(columns, linked=False)
    mtext.text = "".join(add_column_breaks(content))
    return mtext


def make_dynamic_auto_height_columns_r2018(
    content: str,
    width: float,
    gutter_width: float,
    height: float,
    count: int,
    dxfattribs=None,
) -> MText:
    columns = MTextColumns.new_dynamic_auto_height_columns(
        count, width, gutter_width, height
    )
    return _make_dynamic_columns_r2018(content, columns, dxfattribs or {})


def make_dynamic_manual_height_columns_r2018(
    content: str,
    width: float,
    gutter_width: float,
    heights: Sequence[float],
    dxfattribs=None,
) -> MText:
    columns = MTextColumns.new_dynamic_manual_height_columns(
        width, gutter_width, heights
    )
    return _make_dynamic_columns_r2018(content, columns, dxfattribs or {})


def _make_dynamic_columns_r2018(
    content: str, columns: MTextColumns, dxfattribs
) -> MText:
    if not content:
        raise ValueError("no content")
        # column count is not required for DXF R2018
    mtext = MText.new(dxfattribs=dxfattribs)
    mtext.setup_columns(columns, linked=False)
    mtext.text = content
    return mtext
