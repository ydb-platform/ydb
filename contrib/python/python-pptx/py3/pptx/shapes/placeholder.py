"""Placeholder-related objects.

Specific to shapes having a `p:ph` element. A placeholder has distinct behaviors
depending on whether it appears on a slide, layout, or master. Hence there is a
non-trivial class inheritance structure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pptx.enum.shapes import MSO_SHAPE_TYPE, PP_PLACEHOLDER
from pptx.oxml.shapes.graphfrm import CT_GraphicalObjectFrame
from pptx.oxml.shapes.picture import CT_Picture
from pptx.shapes.autoshape import Shape
from pptx.shapes.graphfrm import GraphicFrame
from pptx.shapes.picture import Picture
from pptx.util import Emu

if TYPE_CHECKING:
    from pptx.oxml.shapes.autoshape import CT_Shape


class _InheritsDimensions(object):
    """
    Mixin class that provides inherited dimension behavior. Specifically,
    left, top, width, and height report the value from the layout placeholder
    where they would have otherwise reported |None|. This behavior is
    distinctive to placeholders. :meth:`_base_placeholder` must be overridden
    by all subclasses to provide lookup of the appropriate base placeholder
    to inherit from.
    """

    @property
    def height(self):
        """
        The effective height of this placeholder shape; its directly-applied
        height if it has one, otherwise the height of its parent layout
        placeholder.
        """
        return self._effective_value("height")

    @height.setter
    def height(self, value):
        self._element.cy = value

    @property
    def left(self):
        """
        The effective left of this placeholder shape; its directly-applied
        left if it has one, otherwise the left of its parent layout
        placeholder.
        """
        return self._effective_value("left")

    @left.setter
    def left(self, value):
        self._element.x = value

    @property
    def shape_type(self):
        """
        Member of :ref:`MsoShapeType` specifying the type of this shape.
        Unconditionally ``MSO_SHAPE_TYPE.PLACEHOLDER`` in this case.
        Read-only.
        """
        return MSO_SHAPE_TYPE.PLACEHOLDER

    @property
    def top(self):
        """
        The effective top of this placeholder shape; its directly-applied
        top if it has one, otherwise the top of its parent layout
        placeholder.
        """
        return self._effective_value("top")

    @top.setter
    def top(self, value):
        self._element.y = value

    @property
    def width(self):
        """
        The effective width of this placeholder shape; its directly-applied
        width if it has one, otherwise the width of its parent layout
        placeholder.
        """
        return self._effective_value("width")

    @width.setter
    def width(self, value):
        self._element.cx = value

    @property
    def _base_placeholder(self):
        """
        Return the layout or master placeholder shape this placeholder
        inherits from. Not to be confused with an instance of
        |BasePlaceholder| (necessarily).
        """
        raise NotImplementedError("Must be implemented by all subclasses.")

    def _effective_value(self, attr_name):
        """
        The effective value of *attr_name* on this placeholder shape; its
        directly-applied value if it has one, otherwise the value on the
        layout placeholder it inherits from.
        """
        directly_applied_value = getattr(super(_InheritsDimensions, self), attr_name)
        if directly_applied_value is not None:
            return directly_applied_value
        return self._inherited_value(attr_name)

    def _inherited_value(self, attr_name):
        """
        Return the attribute value, e.g. 'width' of the base placeholder this
        placeholder inherits from.
        """
        base_placeholder = self._base_placeholder
        if base_placeholder is None:
            return None
        inherited_value = getattr(base_placeholder, attr_name)
        return inherited_value


class _BaseSlidePlaceholder(_InheritsDimensions, Shape):
    """Base class for placeholders on slides.

    Provides common behaviors such as inherited dimensions.
    """

    @property
    def is_placeholder(self):
        """
        Boolean indicating whether this shape is a placeholder.
        Unconditionally |True| in this case.
        """
        return True

    @property
    def shape_type(self):
        """
        Member of :ref:`MsoShapeType` specifying the type of this shape.
        Unconditionally ``MSO_SHAPE_TYPE.PLACEHOLDER`` in this case.
        Read-only.
        """
        return MSO_SHAPE_TYPE.PLACEHOLDER

    @property
    def _base_placeholder(self):
        """
        Return the layout placeholder this slide placeholder inherits from.
        Not to be confused with an instance of |BasePlaceholder|
        (necessarily).
        """
        layout, idx = self.part.slide_layout, self._element.ph_idx
        return layout.placeholders.get(idx=idx)

    def _replace_placeholder_with(self, element):
        """
        Substitute *element* for this placeholder element in the shapetree.
        This placeholder's `._element` attribute is set to |None| and its
        original element is free for garbage collection. Any attribute access
        (including a method call) on this placeholder after this call raises
        |AttributeError|.
        """
        element._nvXxPr.nvPr._insert_ph(self._element.ph)
        self._element.addprevious(element)
        self._element.getparent().remove(self._element)
        self._element = None


class BasePlaceholder(Shape):
    """
    NOTE: This class is deprecated and will be removed from a future release
    along with the properties *idx*, *orient*, *ph_type*, and *sz*. The *idx*
    property will be available via the .placeholder_format property. The
    others will be accessed directly from the oxml layer as they are only
    used for internal purposes.

    Base class for placeholder subclasses that differentiate the varying
    behaviors of placeholders on a master, layout, and slide.
    """

    @property
    def idx(self):
        """
        Integer placeholder 'idx' attribute, e.g. 0
        """
        return self._sp.ph_idx

    @property
    def orient(self):
        """
        Placeholder orientation, e.g. ST_Direction.HORZ
        """
        return self._sp.ph_orient

    @property
    def ph_type(self):
        """
        Placeholder type, e.g. PP_PLACEHOLDER.CENTER_TITLE
        """
        return self._sp.ph_type

    @property
    def sz(self):
        """
        Placeholder 'sz' attribute, e.g. ST_PlaceholderSize.FULL
        """
        return self._sp.ph_sz


class LayoutPlaceholder(_InheritsDimensions, Shape):
    """Placeholder shape on a slide layout.

    Provides differentiated behavior for slide layout placeholders, in particular, inheriting
    shape properties from the master placeholder having the same type, when a matching one exists.
    """

    element: CT_Shape  # pyright: ignore[reportIncompatibleMethodOverride]

    @property
    def _base_placeholder(self):
        """
        Return the master placeholder this layout placeholder inherits from.
        """
        base_ph_type = {
            PP_PLACEHOLDER.BODY: PP_PLACEHOLDER.BODY,
            PP_PLACEHOLDER.CHART: PP_PLACEHOLDER.BODY,
            PP_PLACEHOLDER.BITMAP: PP_PLACEHOLDER.BODY,
            PP_PLACEHOLDER.CENTER_TITLE: PP_PLACEHOLDER.TITLE,
            PP_PLACEHOLDER.ORG_CHART: PP_PLACEHOLDER.BODY,
            PP_PLACEHOLDER.DATE: PP_PLACEHOLDER.DATE,
            PP_PLACEHOLDER.FOOTER: PP_PLACEHOLDER.FOOTER,
            PP_PLACEHOLDER.MEDIA_CLIP: PP_PLACEHOLDER.BODY,
            PP_PLACEHOLDER.OBJECT: PP_PLACEHOLDER.BODY,
            PP_PLACEHOLDER.PICTURE: PP_PLACEHOLDER.BODY,
            PP_PLACEHOLDER.SLIDE_NUMBER: PP_PLACEHOLDER.SLIDE_NUMBER,
            PP_PLACEHOLDER.SUBTITLE: PP_PLACEHOLDER.BODY,
            PP_PLACEHOLDER.TABLE: PP_PLACEHOLDER.BODY,
            PP_PLACEHOLDER.TITLE: PP_PLACEHOLDER.TITLE,
        }[self._element.ph_type]
        slide_master = self.part.slide_master
        return slide_master.placeholders.get(base_ph_type, None)


class MasterPlaceholder(BasePlaceholder):
    """Placeholder shape on a slide master."""

    element: CT_Shape  # pyright: ignore[reportIncompatibleMethodOverride]


class NotesSlidePlaceholder(_InheritsDimensions, Shape):
    """
    Placeholder shape on a notes slide. Inherits shape properties from the
    placeholder on the notes master that has the same type (e.g. 'body').
    """

    @property
    def _base_placeholder(self):
        """
        Return the notes master placeholder this notes slide placeholder
        inherits from, or |None| if no placeholder of the matching type is
        present.
        """
        notes_master = self.part.notes_master
        ph_type = self.element.ph_type
        return notes_master.placeholders.get(ph_type=ph_type)


class SlidePlaceholder(_BaseSlidePlaceholder):
    """
    Placeholder shape on a slide. Inherits shape properties from its
    corresponding slide layout placeholder.
    """


class ChartPlaceholder(_BaseSlidePlaceholder):
    """Placeholder shape that can only accept a chart."""

    def insert_chart(self, chart_type, chart_data):
        """
        Return a |PlaceholderGraphicFrame| object containing a new chart of
        *chart_type* depicting *chart_data* and having the same position and
        size as this placeholder. *chart_type* is one of the
        :ref:`XlChartType` enumeration values. *chart_data* is a |ChartData|
        object populated with the categories and series values for the chart.
        Note that the new |Chart| object is not returned directly. The chart
        object may be accessed using the
        :attr:`~.PlaceholderGraphicFrame.chart` property of the returned
        |PlaceholderGraphicFrame| object.
        """
        rId = self.part.add_chart_part(chart_type, chart_data)
        graphicFrame = self._new_chart_graphicFrame(
            rId, self.left, self.top, self.width, self.height
        )
        self._replace_placeholder_with(graphicFrame)
        return PlaceholderGraphicFrame(graphicFrame, self._parent)

    def _new_chart_graphicFrame(self, rId, x, y, cx, cy):
        """
        Return a newly created `p:graphicFrame` element having the specified
        position and size and containing the chart identified by *rId*.
        """
        id_, name = self.shape_id, self.name
        return CT_GraphicalObjectFrame.new_chart_graphicFrame(id_, name, rId, x, y, cx, cy)


class PicturePlaceholder(_BaseSlidePlaceholder):
    """Placeholder shape that can only accept a picture."""

    def insert_picture(self, image_file):
        """Return a |PlaceholderPicture| object depicting the image in `image_file`.

        `image_file` may be either a path (string) or a file-like object. The image is
        cropped to fill the entire space of the placeholder. A |PlaceholderPicture|
        object has all the properties and methods of a |Picture| shape except that the
        value of its :attr:`~._BaseSlidePlaceholder.shape_type` property is
        `MSO_SHAPE_TYPE.PLACEHOLDER` instead of `MSO_SHAPE_TYPE.PICTURE`.
        """
        pic = self._new_placeholder_pic(image_file)
        self._replace_placeholder_with(pic)
        return PlaceholderPicture(pic, self._parent)

    def _new_placeholder_pic(self, image_file):
        """
        Return a new `p:pic` element depicting the image in *image_file*,
        suitable for use as a placeholder. In particular this means not
        having an `a:xfrm` element, allowing its extents to be inherited from
        its layout placeholder.
        """
        rId, desc, image_size = self._get_or_add_image(image_file)
        shape_id, name = self.shape_id, self.name
        pic = CT_Picture.new_ph_pic(shape_id, name, desc, rId)
        pic.crop_to_fit(image_size, (self.width, self.height))
        return pic

    def _get_or_add_image(self, image_file):
        """
        Return an (rId, description, image_size) 3-tuple identifying the
        related image part containing *image_file* and describing the image.
        """
        image_part, rId = self.part.get_or_add_image_part(image_file)
        desc, image_size = image_part.desc, image_part._px_size
        return rId, desc, image_size


class PlaceholderGraphicFrame(GraphicFrame):
    """
    Placeholder shape populated with a table, chart, or smart art.
    """

    @property
    def is_placeholder(self):
        """
        Boolean indicating whether this shape is a placeholder.
        Unconditionally |True| in this case.
        """
        return True


class PlaceholderPicture(_InheritsDimensions, Picture):
    """
    Placeholder shape populated with a picture.
    """

    @property
    def _base_placeholder(self):
        """
        Return the layout placeholder this picture placeholder inherits from.
        """
        layout, idx = self.part.slide_layout, self._element.ph_idx
        return layout.placeholders.get(idx=idx)


class TablePlaceholder(_BaseSlidePlaceholder):
    """Placeholder shape that can only accept a table."""

    def insert_table(self, rows, cols):
        """Return |PlaceholderGraphicFrame| object containing a `rows` by `cols` table.

        The position and width of the table are those of the placeholder and its height
        is proportional to the number of rows. A |PlaceholderGraphicFrame| object has
        all the properties and methods of a |GraphicFrame| shape except that the value
        of its :attr:`~._BaseSlidePlaceholder.shape_type` property is unconditionally
        `MSO_SHAPE_TYPE.PLACEHOLDER`. Note that the return value is not the new table
        but rather *contains* the new table. The table can be accessed using the
        :attr:`~.PlaceholderGraphicFrame.table` property of the returned
        |PlaceholderGraphicFrame| object.
        """
        graphicFrame = self._new_placeholder_table(rows, cols)
        self._replace_placeholder_with(graphicFrame)
        return PlaceholderGraphicFrame(graphicFrame, self._parent)

    def _new_placeholder_table(self, rows, cols):
        """
        Return a newly added `p:graphicFrame` element containing an empty
        table with *rows* rows and *cols* columns, positioned at the location
        of this placeholder and having its same width. The table's height is
        determined by the number of rows.
        """
        shape_id, name, height = self.shape_id, self.name, Emu(rows * 370840)
        return CT_GraphicalObjectFrame.new_table_graphicFrame(
            shape_id, name, rows, cols, self.left, self.top, self.width, height
        )
