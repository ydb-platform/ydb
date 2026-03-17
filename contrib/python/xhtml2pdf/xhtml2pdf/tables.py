# Copyright 2010 Dirk Holtwick, holtwick.it
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import copy
import logging

from reportlab.platypus.tables import TableStyle

from xhtml2pdf.tags import pisaTag
from xhtml2pdf.util import getAlign, getBorderStyle, getSize, set_value
from xhtml2pdf.xhtml2pdf_reportlab import PmlKeepInFrame, PmlTable

log = logging.getLogger(__name__)


def _width(value: str | float | None = None) -> str | float | None:
    if value is None:
        return None
    value = str(value)
    return value if value.endswith("%") else getSize(value)


def _height(value: str | float | None = None) -> str | float | None:
    if value is None:
        return None
    value = str(value)
    return value if value.endswith("%") else getSize(value)


class TableData:
    def __init__(self) -> None:
        self.align: str = ""
        self.col: int = 0
        self.colw: list = []
        self.data: list = []
        self.mode: str = ""
        self.padding: int = 0
        self.repeat: bool = False
        self.row: int = 0
        self.rowh: list = []
        self.span: list = []
        self.styles: list[
            tuple[str, tuple[int, int], tuple[int, int], str, str, str]
        ] = []
        self.width: str | float = 0

    def add_cell(self, data=None):
        self.col += 1
        self.data[len(self.data) - 1].append(data)

    def add_style(self, data):
        self.styles.append(copy.copy(data))

    def add_empty(self, x, y):
        self.span.append((x, y))

    def get_data(self):
        data = self.data
        for x, y in self.span:
            # Loop through all the spans that are inside the boundaries of our
            # tables. If the y-coordinate is valid, we insert an empty cell.
            # As for the x coordinate, we somehow don't care.
            if y < len(data):
                data[y].insert(x, "")
        return data

    def add_cell_styles(self, c, begin, end, mode="td"):
        self.mode = mode.upper()
        if c.frag.backColor and mode != "tr":  # XXX Stimmt das so?
            self.add_style(("BACKGROUND", begin, end, c.frag.backColor))

        if 0:
            log.debug(
                "%r",
                (
                    begin,
                    end,
                    c.frag.borderTopWidth,
                    c.frag.borderTopStyle,
                    c.frag.borderTopColor,
                    c.frag.borderBottomWidth,
                    c.frag.borderBottomStyle,
                    c.frag.borderBottomColor,
                    c.frag.borderLeftWidth,
                    c.frag.borderLeftStyle,
                    c.frag.borderLeftColor,
                    c.frag.borderRightWidth,
                    c.frag.borderRightStyle,
                    c.frag.borderRightColor,
                ),
            )

        if (
            getBorderStyle(c.frag.borderTopStyle)
            and c.frag.borderTopWidth
            and c.frag.borderTopColor is not None
        ):
            self.add_style(
                (
                    "LINEABOVE",
                    begin,
                    (end[0], begin[1]),
                    c.frag.borderTopWidth,
                    c.frag.borderTopColor,
                    "squared",
                )
            )
        if (
            getBorderStyle(c.frag.borderLeftStyle)
            and c.frag.borderLeftWidth
            and c.frag.borderLeftColor is not None
        ):
            self.add_style(
                (
                    "LINEBEFORE",
                    begin,
                    (begin[0], end[1]),
                    c.frag.borderLeftWidth,
                    c.frag.borderLeftColor,
                    "squared",
                )
            )
        if (
            getBorderStyle(c.frag.borderRightStyle)
            and c.frag.borderRightWidth
            and c.frag.borderRightColor is not None
        ):
            self.add_style(
                (
                    "LINEAFTER",
                    (end[0], begin[1]),
                    end,
                    c.frag.borderRightWidth,
                    c.frag.borderRightColor,
                    "squared",
                )
            )
        if (
            getBorderStyle(c.frag.borderBottomStyle)
            and c.frag.borderBottomWidth
            and c.frag.borderBottomColor is not None
        ):
            self.add_style(
                (
                    "LINEBELOW",
                    (begin[0], end[1]),
                    end,
                    c.frag.borderBottomWidth,
                    c.frag.borderBottomColor,
                    "squared",
                )
            )
        self.add_style(("LEFTPADDING", begin, end, c.frag.paddingLeft or self.padding))
        self.add_style(
            ("RIGHTPADDING", begin, end, c.frag.paddingRight or self.padding)
        )
        self.add_style(("TOPPADDING", begin, end, c.frag.paddingTop or self.padding))
        self.add_style(
            ("BOTTOMPADDING", begin, end, c.frag.paddingBottom or self.padding)
        )


class pisaTagTABLE(pisaTag):
    @staticmethod
    def set_borders(frag, attrs):
        set_value(
            frag,
            (
                "borderLeftColor",
                "borderRightColor",
                "borderTopColor",
                "borderBottomColor",
            ),
            attrs.bordercolor,
        )

        set_value(
            frag,
            (
                "borderLeftWidth",
                "borderRightWidth",
                "borderTopWidth",
                "borderBottomWidth",
            ),
            attrs.border,
        )
        set_value(
            frag,
            (
                "borderBottomStyle",
                "borderLeftStyle",
                "borderTopStyle",
                "borderRightStyle",
            ),
            "solid",
        )

    def start(self, c):
        c.addPara()

        attrs = self.attr

        c.tableData, self.tableData = TableData(), c.tableData
        tdata = c.tableData

        if attrs.border and attrs.bordercolor:
            self.set_borders(c.frag, attrs)

        tdata.padding = attrs.cellpadding
        tdata.add_cell_styles(c, (0, 0), (-1, -1), "table")
        tdata.align = attrs.align.upper()
        tdata.col = 0
        tdata.row = 0
        tdata.colw = []
        tdata.rowh = []
        tdata.repeat = attrs.repeat
        tdata.width = _width(attrs.width)

    def end(self, c):
        tdata = c.tableData
        data = tdata.get_data()

        # Add missing columns so that each row has the same count of columns
        # This prevents errors in Reportlab table

        try:
            maxcols = max([len(row) for row in data] or [0])
        except ValueError:
            log.warning(c.warning("<table> rows seem to be inconsistent"))
            maxcols = [0]

        for i, row in enumerate(data):
            data[i] += [""] * (maxcols - len(row))

        log.debug("Col widths: %r", tdata.colw)
        if tdata.data:
            # log.debug("Table styles %r", tdata.styles)
            t = PmlTable(
                data,
                colWidths=tdata.colw,
                rowHeights=tdata.rowh,
                # totalWidth = tdata.width,
                splitByRow=1,
                # repeatCols = 1,
                repeatRows=tdata.repeat,
                hAlign=tdata.align,
                vAlign="TOP",
                style=TableStyle(tdata.styles),
            )
            t.totalWidth = _width(tdata.width)
            t.spaceBefore = c.frag.spaceBefore
            t.spaceAfter = c.frag.spaceAfter

            # XXX Maybe we need to copy some more properties?
            t.keepWithNext = c.frag.keepWithNext
            # t.hAlign = tdata.align
            c.addStory(t)
        else:
            log.warning(c.warning("<table> is empty"))

        # Cleanup and re-swap table data
        c.clearFrag()
        c.tableData, self.tableData = self.tableData, None


class pisaTagTR(pisaTag):
    def start(self, c):
        tdata = c.tableData
        row = tdata.row
        begin = (0, row)
        end = (-1, row)

        tdata.add_cell_styles(c, begin, end, "tr")
        c.frag.vAlign = self.attr.valign or c.frag.vAlign
        if c.frag.backColor:
            tdata.add_style(("BACKGROUND", begin, end, c.frag.backColor))

        tdata.col = 0
        tdata.data.append([])

    @staticmethod
    def end(c):
        c.tableData.row += 1


class pisaTagTD(pisaTag):
    def start(self, c):
        if self.attr.align is not None:
            c.frag.alignment = getAlign(self.attr.align)

        c.clearFrag()
        self.story = c.swapStory()

        attrs = self.attr

        tdata = c.tableData

        cspan = attrs.colspan
        rspan = attrs.rowspan

        row = tdata.row
        col = tdata.col
        while 1:
            for x, y in tdata.span:
                if x == col and y == row:
                    col += 1
                    tdata.col += 1
            break

        begin = (col, row)
        end = (col, row)
        if cspan:
            end = (end[0] + cspan - 1, end[1])
        if rspan:
            end = (end[0], end[1] + rspan - 1)
        if begin != end:
            tdata.add_style(("SPAN", begin, end))
            for x in range(begin[0], end[0] + 1):
                for y in range(begin[1], end[1] + 1):
                    if x != begin[0] or y != begin[1]:
                        tdata.add_empty(x, y)

        # Set Border and padding styles
        tdata.add_cell_styles(c, begin, end, "td")

        # Calculate widths
        # Add empty placeholders for new columns
        if (col + 1) > len(tdata.colw):
            tdata.colw += (col + 1 - len(tdata.colw)) * [_width()]

        # Get value of with, if no spanning
        if not cspan:
            width = c.frag.width or self.attr.width
            # If is value, the set it in the right place in the array
            if width is not None:
                tdata.colw[col] = _width(width)
                log.debug("Col %d has width %s", col, width)
            else:
                # If there are no child nodes, nothing within the column can change the
                # width.  Set the column width to the sum of the right and left padding
                # rather than letting it default.
                log.debug(width)
                if len(self.node.childNodes) == 0:
                    width = c.frag.paddingLeft + c.frag.paddingRight
                    log.debug("Col %d has width %s", col, width)
                    if width:
                        tdata.colw[col] = _width(width)
                else:
                    # Child nodes are present, we cannot do anything about the
                    # width except set it externally.
                    pass

        # Calculate heights
        if row + 1 > len(tdata.rowh):
            tdata.rowh += (row + 1 - len(tdata.rowh)) * [_width()]
        if not rspan:
            height = c.frag.height or self.attr.get("height", None)
            if height is not None:
                tdata.rowh[row] = _height(height)
                tdata.add_style(("FONTSIZE", begin, end, 1.0))
                tdata.add_style(("LEADING", begin, end, 1.0))

        # Vertical align
        valign = self.attr.valign or c.frag.vAlign
        if valign is not None:
            tdata.add_style(("VALIGN", begin, end, valign.upper()))

        # Reset border, otherwise the paragraph block will have borders too
        frag = c.frag

        set_value(
            frag,
            (
                "borderLeftWidth",
                "borderRightWidth",
                "borderTopWidth",
                "borderBottomWidth",
            ),
            0,
        )

        set_value(
            frag,
            (
                "borderLeftColor",
                "borderLeftStyle",
                "borderRightColor",
                "borderRightStyle",
                "borderTopColor",
                "borderTopStyle",
                "borderBottomColor",
                "borderBottomStyle",
            ),
            None,
        )

    def end(self, c):
        tdata = c.tableData

        c.addPara()
        cell = c.story

        # Keep in frame if needed since Reportlab does no split inside of cells
        if not c.frag.insideStaticFrame:
            # tdata.keepinframe["content"] = cell
            mode = c.cssAttr.get("-pdf-keep-in-frame-mode", "shrink")
            # keepInFrame mode is passed to Platypus for rendering
            cell = PmlKeepInFrame(maxWidth=0, maxHeight=0, mode=mode, content=cell)

        c.swapStory(self.story)

        tdata.add_cell(cell)


class pisaTagTH(pisaTagTD):
    pass
