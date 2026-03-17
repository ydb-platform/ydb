"""Connector (line) shape and related objects.

A connector is a line shape having end-points that can be connected to other
objects (but not to other connectors). A connector can be straight, have
elbows, or can be curved.
"""

from __future__ import annotations

from pptx.dml.line import LineFormat
from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.shapes.base import BaseShape
from pptx.util import Emu, lazyproperty


class Connector(BaseShape):
    """Connector (line) shape.

    A connector is a linear shape having end-points that can be connected to
    other objects (but not to other connectors). A connector can be straight,
    have elbows, or can be curved.
    """

    def begin_connect(self, shape, cxn_pt_idx):
        """
        **EXPERIMENTAL** - *The current implementation only works properly
        with rectangular shapes, such as pictures and rectangles. Use with
        other shape types may cause unexpected visual alignment of the
        connected end-point and could lead to a load error if cxn_pt_idx
        exceeds the connection point count available on the connected shape.
        That said, a quick test should reveal what to expect when using this
        method with other shape types.*

        Connect the beginning of this connector to *shape* at the connection
        point specified by *cxn_pt_idx*. Each shape has zero or more
        connection points and they are identified by index, starting with 0.
        Generally, the first connection point of a shape is at the top center
        of its bounding box and numbering proceeds counter-clockwise from
        there. However this is only a convention and may vary, especially
        with non built-in shapes.
        """
        self._connect_begin_to(shape, cxn_pt_idx)
        self._move_begin_to_cxn(shape, cxn_pt_idx)

    @property
    def begin_x(self):
        """
        Return the X-position of the begin point of this connector, in
        English Metric Units (as a |Length| object).
        """
        cxnSp = self._element
        x, cx, flipH = cxnSp.x, cxnSp.cx, cxnSp.flipH
        begin_x = x + cx if flipH else x
        return Emu(begin_x)

    @begin_x.setter
    def begin_x(self, value):
        cxnSp = self._element
        x, cx, flipH, new_x = cxnSp.x, cxnSp.cx, cxnSp.flipH, int(value)

        if flipH:
            old_x = x + cx
            dx = abs(new_x - old_x)
            if new_x >= old_x:
                cxnSp.cx = cx + dx
            elif dx <= cx:
                cxnSp.cx = cx - dx
            else:
                cxnSp.flipH = False
                cxnSp.x = new_x
                cxnSp.cx = dx - cx
        else:
            dx = abs(new_x - x)
            if new_x <= x:
                cxnSp.x = new_x
                cxnSp.cx = cx + dx
            elif dx <= cx:
                cxnSp.x = new_x
                cxnSp.cx = cx - dx
            else:
                cxnSp.flipH = True
                cxnSp.x = x + cx
                cxnSp.cx = dx - cx

    @property
    def begin_y(self):
        """
        Return the Y-position of the begin point of this connector, in
        English Metric Units (as a |Length| object).
        """
        cxnSp = self._element
        y, cy, flipV = cxnSp.y, cxnSp.cy, cxnSp.flipV
        begin_y = y + cy if flipV else y
        return Emu(begin_y)

    @begin_y.setter
    def begin_y(self, value):
        cxnSp = self._element
        y, cy, flipV, new_y = cxnSp.y, cxnSp.cy, cxnSp.flipV, int(value)

        if flipV:
            old_y = y + cy
            dy = abs(new_y - old_y)
            if new_y >= old_y:
                cxnSp.cy = cy + dy
            elif dy <= cy:
                cxnSp.cy = cy - dy
            else:
                cxnSp.flipV = False
                cxnSp.y = new_y
                cxnSp.cy = dy - cy
        else:
            dy = abs(new_y - y)
            if new_y <= y:
                cxnSp.y = new_y
                cxnSp.cy = cy + dy
            elif dy <= cy:
                cxnSp.y = new_y
                cxnSp.cy = cy - dy
            else:
                cxnSp.flipV = True
                cxnSp.y = y + cy
                cxnSp.cy = dy - cy

    def end_connect(self, shape, cxn_pt_idx):
        """
        **EXPERIMENTAL** - *The current implementation only works properly
        with rectangular shapes, such as pictures and rectangles. Use with
        other shape types may cause unexpected visual alignment of the
        connected end-point and could lead to a load error if cxn_pt_idx
        exceeds the connection point count available on the connected shape.
        That said, a quick test should reveal what to expect when using this
        method with other shape types.*

        Connect the ending of this connector to *shape* at the connection
        point specified by *cxn_pt_idx*.
        """
        self._connect_end_to(shape, cxn_pt_idx)
        self._move_end_to_cxn(shape, cxn_pt_idx)

    @property
    def end_x(self):
        """
        Return the X-position of the end point of this connector, in English
        Metric Units (as a |Length| object).
        """
        cxnSp = self._element
        x, cx, flipH = cxnSp.x, cxnSp.cx, cxnSp.flipH
        end_x = x if flipH else x + cx
        return Emu(end_x)

    @end_x.setter
    def end_x(self, value):
        cxnSp = self._element
        x, cx, flipH, new_x = cxnSp.x, cxnSp.cx, cxnSp.flipH, int(value)

        if flipH:
            dx = abs(new_x - x)
            if new_x <= x:
                cxnSp.x = new_x
                cxnSp.cx = cx + dx
            elif dx <= cx:
                cxnSp.x = new_x
                cxnSp.cx = cx - dx
            else:
                cxnSp.flipH = False
                cxnSp.x = x + cx
                cxnSp.cx = dx - cx
        else:
            old_x = x + cx
            dx = abs(new_x - old_x)
            if new_x >= old_x:
                cxnSp.cx = cx + dx
            elif dx <= cx:
                cxnSp.cx = cx - dx
            else:
                cxnSp.flipH = True
                cxnSp.x = new_x
                cxnSp.cx = dx - cx

    @property
    def end_y(self):
        """
        Return the Y-position of the end point of this connector, in English
        Metric Units (as a |Length| object).
        """
        cxnSp = self._element
        y, cy, flipV = cxnSp.y, cxnSp.cy, cxnSp.flipV
        end_y = y if flipV else y + cy
        return Emu(end_y)

    @end_y.setter
    def end_y(self, value):
        cxnSp = self._element
        y, cy, flipV, new_y = cxnSp.y, cxnSp.cy, cxnSp.flipV, int(value)

        if flipV:
            dy = abs(new_y - y)
            if new_y <= y:
                cxnSp.y = new_y
                cxnSp.cy = cy + dy
            elif dy <= cy:
                cxnSp.y = new_y
                cxnSp.cy = cy - dy
            else:
                cxnSp.flipV = False
                cxnSp.y = y + cy
                cxnSp.cy = dy - cy
        else:
            old_y = y + cy
            dy = abs(new_y - old_y)
            if new_y >= old_y:
                cxnSp.cy = cy + dy
            elif dy <= cy:
                cxnSp.cy = cy - dy
            else:
                cxnSp.flipV = True
                cxnSp.y = new_y
                cxnSp.cy = dy - cy

    def get_or_add_ln(self):
        """Helper method required by |LineFormat|."""
        return self._element.spPr.get_or_add_ln()

    @lazyproperty
    def line(self):
        """|LineFormat| instance for this connector.

        Provides access to line properties such as line color, width, and
        line style.
        """
        return LineFormat(self)

    @property
    def ln(self):
        """Helper method required by |LineFormat|.

        The ``<a:ln>`` element containing the line format properties such as
        line color and width. |None| if no `<a:ln>` element is present.
        """
        return self._element.spPr.ln

    @property
    def shape_type(self):
        """Member of `MSO_SHAPE_TYPE` identifying the type of this shape.

        Unconditionally `MSO_SHAPE_TYPE.LINE` for a `Connector` object.
        """
        return MSO_SHAPE_TYPE.LINE

    def _connect_begin_to(self, shape, cxn_pt_idx):
        """
        Add or update a stCxn element for this connector that connects its
        begin point to the connection point of *shape* specified by
        *cxn_pt_idx*.
        """
        cNvCxnSpPr = self._element.nvCxnSpPr.cNvCxnSpPr
        stCxn = cNvCxnSpPr.get_or_add_stCxn()
        stCxn.id = shape.shape_id
        stCxn.idx = cxn_pt_idx

    def _connect_end_to(self, shape, cxn_pt_idx):
        """
        Add or update an endCxn element for this connector that connects its
        end point to the connection point of *shape* specified by
        *cxn_pt_idx*.
        """
        cNvCxnSpPr = self._element.nvCxnSpPr.cNvCxnSpPr
        endCxn = cNvCxnSpPr.get_or_add_endCxn()
        endCxn.id = shape.shape_id
        endCxn.idx = cxn_pt_idx

    def _move_begin_to_cxn(self, shape, cxn_pt_idx):
        """
        Move the begin point of this connector to coordinates of the
        connection point of *shape* specified by *cxn_pt_idx*.
        """
        x, y, cx, cy = shape.left, shape.top, shape.width, shape.height
        self.begin_x, self.begin_y = {
            0: (int(x + cx / 2), y),
            1: (x, int(y + cy / 2)),
            2: (int(x + cx / 2), y + cy),
            3: (x + cx, int(y + cy / 2)),
        }[cxn_pt_idx]

    def _move_end_to_cxn(self, shape, cxn_pt_idx):
        """
        Move the end point of this connector to the coordinates of the
        connection point of *shape* specified by *cxn_pt_idx*.
        """
        x, y, cx, cy = shape.left, shape.top, shape.width, shape.height
        self.end_x, self.end_y = {
            0: (int(x + cx / 2), y),
            1: (x, int(y + cy / 2)),
            2: (int(x + cx / 2), y + cy),
            3: (x + cx, int(y + cy / 2)),
        }[cxn_pt_idx]
