# encoding: utf-8

"""Graphic Frame shape and related objects.

A graphic frame is a common container for table, chart, smart art, and media
objects.
"""

from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.shapes.base import BaseShape
from pptx.shared import ParentedElementProxy
from pptx.spec import (
    GRAPHIC_DATA_URI_CHART,
    GRAPHIC_DATA_URI_OLEOBJ,
    GRAPHIC_DATA_URI_TABLE,
)
from pptx.table import Table


class GraphicFrame(BaseShape):
    """Container shape for table, chart, smart art, and media objects.

    Corresponds to a ``<p:graphicFrame>`` element in the shape tree.
    """

    @property
    def chart(self):
        """The |Chart| object containing the chart in this graphic frame.

        Raises |ValueError| if this graphic frame does not contain a chart.
        """
        if not self.has_chart:
            raise ValueError("shape does not contain a chart")
        return self.chart_part.chart

    @property
    def chart_part(self):
        """The |ChartPart| object containing the chart in this graphic frame."""
        return self.part.related_part(self._element.chart_rId)

    @property
    def has_chart(self):
        """|True| if this graphic frame contains a chart object. |False| otherwise.

        When |True|, the chart object can be accessed using the ``.chart`` property.
        """
        return self._element.graphicData_uri == GRAPHIC_DATA_URI_CHART

    @property
    def has_table(self):
        """|True| if this graphic frame contains a table object, |False| otherwise.

        When |True|, the table object can be accessed using the `.table` property.
        """
        return self._element.graphicData_uri == GRAPHIC_DATA_URI_TABLE

    @property
    def ole_format(self):
        """Optional _OleFormat object for this graphic-frame shape.

        Raises `ValueError` on a GraphicFrame instance that does not contain an OLE
        object.

        An shape that contains an OLE object will have `.shape_type` of either
        `EMBEDDED_OLE_OBJECT` or `LINKED_OLE_OBJECT`.
        """
        if not self._element.has_oleobj:
            raise ValueError("not an OLE-object shape")
        return _OleFormat(self._element.graphicData, self._parent)

    @property
    def shadow(self):
        """Unconditionally raises |NotImplementedError|.

        Access to the shadow effect for graphic-frame objects is
        content-specific (i.e. different for charts, tables, etc.) and has
        not yet been implemented.
        """
        raise NotImplementedError("shadow property on GraphicFrame not yet supported")

    @property
    def shape_type(self):
        """Optional member of `MSO_SHAPE_TYPE` identifying the type of this shape.

        Possible values are ``MSO_SHAPE_TYPE.CHART``, ``MSO_SHAPE_TYPE.TABLE``,
        ``MSO_SHAPE_TYPE.EMBEDDED_OLE_OBJECT``, ``MSO_SHAPE_TYPE.LINKED_OLE_OBJECT``.

        This value is `None` when none of these four types apply, for example when the
        shape contains SmartArt.
        """
        graphicData_uri = self._element.graphicData_uri
        if graphicData_uri == GRAPHIC_DATA_URI_CHART:
            return MSO_SHAPE_TYPE.CHART
        elif graphicData_uri == GRAPHIC_DATA_URI_TABLE:
            return MSO_SHAPE_TYPE.TABLE
        elif graphicData_uri == GRAPHIC_DATA_URI_OLEOBJ:
            return (
                MSO_SHAPE_TYPE.EMBEDDED_OLE_OBJECT
                if self._element.is_embedded_ole_obj
                else MSO_SHAPE_TYPE.LINKED_OLE_OBJECT
            )
        else:
            return None

    @property
    def table(self):
        """
        The |Table| object contained in this graphic frame. Raises
        |ValueError| if this graphic frame does not contain a table.
        """
        if not self.has_table:
            raise ValueError("shape does not contain a table")
        tbl = self._element.graphic.graphicData.tbl
        return Table(tbl, self)


class _OleFormat(ParentedElementProxy):
    """Provides attributes on an embedded OLE object."""

    def __init__(self, graphicData, parent):
        super(_OleFormat, self).__init__(graphicData, parent)
        self._graphicData = graphicData

    @property
    def blob(self):
        """Optional bytes of OLE object, suitable for loading or saving as a file.

        This value is None if the embedded object does not represent a "file".
        """
        return self.part.related_part(self._graphicData.blob_rId).blob

    @property
    def prog_id(self):
        """str "progId" attribute of this embedded OLE object.

        The progId is a str like "Excel.Sheet.12" that identifies the "file-type" of the
        embedded object, or perhaps more precisely, the application (aka. "server" in
        OLE parlance) to be used to open this object.
        """
        return self._graphicData.progId

    @property
    def show_as_icon(self):
        """True when OLE object should appear as an icon (rather than preview)."""
        return self._graphicData.showAsIcon
