"""The shape tree, the structure that holds a slide's shapes."""

from __future__ import annotations

import io
import os
from typing import IO, TYPE_CHECKING, Callable, Iterable, Iterator, cast

from pptx.enum.shapes import PP_PLACEHOLDER, PROG_ID
from pptx.media import SPEAKER_IMAGE_BYTES, Video
from pptx.opc.constants import CONTENT_TYPE as CT
from pptx.oxml.ns import qn
from pptx.oxml.shapes.autoshape import CT_Shape
from pptx.oxml.shapes.graphfrm import CT_GraphicalObjectFrame
from pptx.oxml.shapes.picture import CT_Picture
from pptx.oxml.simpletypes import ST_Direction
from pptx.shapes.autoshape import AutoShapeType, Shape
from pptx.shapes.base import BaseShape
from pptx.shapes.connector import Connector
from pptx.shapes.freeform import FreeformBuilder
from pptx.shapes.graphfrm import GraphicFrame
from pptx.shapes.group import GroupShape
from pptx.shapes.picture import Movie, Picture
from pptx.shapes.placeholder import (
    ChartPlaceholder,
    LayoutPlaceholder,
    MasterPlaceholder,
    NotesSlidePlaceholder,
    PicturePlaceholder,
    PlaceholderGraphicFrame,
    PlaceholderPicture,
    SlidePlaceholder,
    TablePlaceholder,
)
from pptx.shared import ParentedElementProxy
from pptx.util import Emu, lazyproperty

if TYPE_CHECKING:
    from pptx.chart.chart import Chart
    from pptx.chart.data import ChartData
    from pptx.enum.chart import XL_CHART_TYPE
    from pptx.enum.shapes import MSO_CONNECTOR_TYPE, MSO_SHAPE
    from pptx.oxml.shapes import ShapeElement
    from pptx.oxml.shapes.connector import CT_Connector
    from pptx.oxml.shapes.groupshape import CT_GroupShape
    from pptx.parts.image import ImagePart
    from pptx.parts.slide import SlidePart
    from pptx.slide import Slide, SlideLayout
    from pptx.types import ProvidesPart
    from pptx.util import Length

# +-- _BaseShapes
# |   |
# |   +-- _BaseGroupShapes
# |   |   |
# |   |   +-- GroupShapes
# |   |   |
# |   |   +-- SlideShapes
# |   |
# |   +-- LayoutShapes
# |   |
# |   +-- MasterShapes
# |   |
# |   +-- NotesSlideShapes
# |   |
# |   +-- BasePlaceholders
# |       |
# |       +-- LayoutPlaceholders
# |       |
# |       +-- MasterPlaceholders
# |           |
# |           +-- NotesSlidePlaceholders
# |
# +-- SlidePlaceholders


class _BaseShapes(ParentedElementProxy):
    """Base class for a shape collection appearing in a slide-type object.

    Subclasses include Slide, SlideLayout, and SlideMaster. Provides common methods.
    """

    def __init__(self, spTree: CT_GroupShape, parent: ProvidesPart):
        super(_BaseShapes, self).__init__(spTree, parent)
        self._spTree = spTree
        self._cached_max_shape_id = None

    def __getitem__(self, idx: int) -> BaseShape:
        """Return shape at `idx` in sequence, e.g. `shapes[2]`."""
        shape_elms = list(self._iter_member_elms())
        try:
            shape_elm = shape_elms[idx]
        except IndexError:
            raise IndexError("shape index out of range")
        return self._shape_factory(shape_elm)

    def __iter__(self) -> Iterator[BaseShape]:
        """Generate a reference to each shape in the collection, in sequence."""
        for shape_elm in self._iter_member_elms():
            yield self._shape_factory(shape_elm)

    def __len__(self) -> int:
        """Return count of shapes in this shape tree.

        A group shape contributes 1 to the total, without regard to the number of shapes contained
        in the group.
        """
        shape_elms = list(self._iter_member_elms())
        return len(shape_elms)

    def clone_placeholder(self, placeholder: LayoutPlaceholder) -> None:
        """Add a new placeholder shape based on `placeholder`."""
        sp = placeholder.element
        ph_type, orient, sz, idx = (sp.ph_type, sp.ph_orient, sp.ph_sz, sp.ph_idx)
        id_ = self._next_shape_id
        name = self._next_ph_name(ph_type, id_, orient)
        self._spTree.add_placeholder(id_, name, ph_type, orient, sz, idx)

    def ph_basename(self, ph_type: PP_PLACEHOLDER) -> str:
        """Return the base name for a placeholder of `ph_type` in this shape collection.

        There is some variance between slide types, for example a notes slide uses a different
        name for the body placeholder, so this method can be overriden by subclasses.
        """
        return {
            PP_PLACEHOLDER.BITMAP: "ClipArt Placeholder",
            PP_PLACEHOLDER.BODY: "Text Placeholder",
            PP_PLACEHOLDER.CENTER_TITLE: "Title",
            PP_PLACEHOLDER.CHART: "Chart Placeholder",
            PP_PLACEHOLDER.DATE: "Date Placeholder",
            PP_PLACEHOLDER.FOOTER: "Footer Placeholder",
            PP_PLACEHOLDER.HEADER: "Header Placeholder",
            PP_PLACEHOLDER.MEDIA_CLIP: "Media Placeholder",
            PP_PLACEHOLDER.OBJECT: "Content Placeholder",
            PP_PLACEHOLDER.ORG_CHART: "SmartArt Placeholder",
            PP_PLACEHOLDER.PICTURE: "Picture Placeholder",
            PP_PLACEHOLDER.SLIDE_NUMBER: "Slide Number Placeholder",
            PP_PLACEHOLDER.SUBTITLE: "Subtitle",
            PP_PLACEHOLDER.TABLE: "Table Placeholder",
            PP_PLACEHOLDER.TITLE: "Title",
        }[ph_type]

    @property
    def turbo_add_enabled(self) -> bool:
        """True if "turbo-add" mode is enabled. Read/Write.

        EXPERIMENTAL: This feature can radically improve performance when adding large numbers
        (hundreds of shapes) to a slide. It works by caching the last shape ID used and
        incrementing that value to assign the next shape id. This avoids repeatedly searching all
        shape ids in the slide each time a new ID is required.

        Performance is not noticeably improved for a slide with a relatively small number of
        shapes, but because the search time rises with the square of the shape count, this option
        can be useful for optimizing generation of a slide composed of many shapes.

        Shape-id collisions can occur (causing a repair error on load) if more than one |Slide|
        object is used to interact with the same slide in the presentation. Note that the |Slides|
        collection creates a new |Slide| object each time a slide is accessed (e.g. `slide =
        prs.slides[0]`, so you must be careful to limit use to a single |Slide| object.
        """
        return self._cached_max_shape_id is not None

    @turbo_add_enabled.setter
    def turbo_add_enabled(self, value: bool):
        enable = bool(value)
        self._cached_max_shape_id = self._spTree.max_shape_id if enable else None

    @staticmethod
    def _is_member_elm(shape_elm: ShapeElement) -> bool:
        """Return true if `shape_elm` represents a member of this collection, False otherwise."""
        return True

    def _iter_member_elms(self) -> Iterator[ShapeElement]:
        """Generate each child of the `p:spTree` element that corresponds to a shape.

        Items appear in XML document order.
        """
        for shape_elm in self._spTree.iter_shape_elms():
            if self._is_member_elm(shape_elm):
                yield shape_elm

    def _next_ph_name(self, ph_type: PP_PLACEHOLDER, id: int, orient: str) -> str:
        """Next unique placeholder name for placeholder shape of type `ph_type`.

        Usually will be standard placeholder root name suffixed with id-1, e.g.
        _next_ph_name(ST_PlaceholderType.TBL, 4, 'horz') ==> 'Table Placeholder 3'. The number is
        incremented as necessary to make the name unique within the collection. If `orient` is
        `'vert'`, the placeholder name is prefixed with `'Vertical '`.
        """
        basename = self.ph_basename(ph_type)

        # prefix rootname with 'Vertical ' if orient is 'vert'
        if orient == ST_Direction.VERT:
            basename = "Vertical %s" % basename

        # increment numpart as necessary to make name unique
        numpart = id - 1
        names = self._spTree.xpath("//p:cNvPr/@name")
        while True:
            name = "%s %d" % (basename, numpart)
            if name not in names:
                break
            numpart += 1

        return name

    @property
    def _next_shape_id(self) -> int:
        """Return a unique shape id suitable for use with a new shape.

        The returned id is 1 greater than the maximum shape id used so far. In practice, the
        minimum id is 2 because the spTree element is always assigned id="1".
        """
        # ---presence of cached-max-shape-id indicates turbo mode is on---
        if self._cached_max_shape_id is not None:
            self._cached_max_shape_id += 1
            return self._cached_max_shape_id

        return self._spTree.max_shape_id + 1

    def _shape_factory(self, shape_elm: ShapeElement) -> BaseShape:
        """Return an instance of the appropriate shape proxy class for `shape_elm`."""
        return BaseShapeFactory(shape_elm, self)


class _BaseGroupShapes(_BaseShapes):
    """Base class for shape-trees that can add shapes."""

    part: SlidePart  # pyright: ignore[reportIncompatibleMethodOverride]
    _element: CT_GroupShape

    def __init__(self, grpSp: CT_GroupShape, parent: ProvidesPart):
        super(_BaseGroupShapes, self).__init__(grpSp, parent)
        self._grpSp = grpSp

    def add_chart(
        self,
        chart_type: XL_CHART_TYPE,
        x: Length,
        y: Length,
        cx: Length,
        cy: Length,
        chart_data: ChartData,
    ) -> Chart:
        """Add a new chart of `chart_type` to the slide.

        The chart is positioned at (`x`, `y`), has size (`cx`, `cy`), and depicts `chart_data`.
        `chart_type` is one of the :ref:`XlChartType` enumeration values. `chart_data` is a
        |ChartData| object populated with the categories and series values for the chart.

        Note that a |GraphicFrame| shape object is returned, not the |Chart| object contained in
        that graphic frame shape. The chart object may be accessed using the :attr:`chart`
        property of the returned |GraphicFrame| object.
        """
        rId = self.part.add_chart_part(chart_type, chart_data)
        graphicFrame = self._add_chart_graphicFrame(rId, x, y, cx, cy)
        self._recalculate_extents()
        return cast("Chart", self._shape_factory(graphicFrame))

    def add_connector(
        self,
        connector_type: MSO_CONNECTOR_TYPE,
        begin_x: Length,
        begin_y: Length,
        end_x: Length,
        end_y: Length,
    ) -> Connector:
        """Add a newly created connector shape to the end of this shape tree.

        `connector_type` is a member of the :ref:`MsoConnectorType` enumeration and the end-point
        values are specified as EMU values. The returned connector is of type `connector_type` and
        has begin and end points as specified.
        """
        cxnSp = self._add_cxnSp(connector_type, begin_x, begin_y, end_x, end_y)
        self._recalculate_extents()
        return cast(Connector, self._shape_factory(cxnSp))

    def add_group_shape(self, shapes: Iterable[BaseShape] = ()) -> GroupShape:
        """Return a |GroupShape| object newly appended to this shape tree.

        The group shape is empty and must be populated with shapes using methods on its shape
        tree, available on its `.shapes` property. The position and extents of the group shape are
        determined by the shapes it contains; its position and extents are recalculated each time
        a shape is added to it.
        """
        shapes = tuple(shapes)
        grpSp = self._element.add_grpSp()
        for shape in shapes:
            grpSp.insert_element_before(
                shape._element, "p:extLst"  # pyright: ignore[reportPrivateUsage]
            )
        if shapes:
            grpSp.recalculate_extents()
        return cast(GroupShape, self._shape_factory(grpSp))

    def add_ole_object(
        self,
        object_file: str | IO[bytes],
        prog_id: str,
        left: Length,
        top: Length,
        width: Length | None = None,
        height: Length | None = None,
        icon_file: str | IO[bytes] | None = None,
        icon_width: Length | None = None,
        icon_height: Length | None = None,
    ) -> GraphicFrame:
        """Return newly-created GraphicFrame shape embedding `object_file`.

        The returned graphic-frame shape contains `object_file` as an embedded OLE object. It is
        displayed as an icon at `left`, `top` with size `width`, `height`. `width` and `height`
        may be omitted when `prog_id` is a member of `PROG_ID`, in which case the default icon
        size is used. This is advised for best appearance where applicable because it avoids an
        icon with a "stretched" appearance.

        `object_file` may either be a str path to a file or file-like object (such as
        `io.BytesIO`) containing the bytes of the object to be embedded (such as an Excel file).

        `prog_id` can be either a member of `pptx.enum.shapes.PROG_ID` or a str value like
        `"Adobe.Exchange.7"` determined by inspecting the XML generated by PowerPoint for an
        object of the desired type.

        `icon_file` may either be a str path to an image file or a file-like object containing the
        image. The image provided will be displayed in lieu of the OLE object; double-clicking on
        the image opens the object (subject to operating-system limitations). The image file can
        be any supported image file. Those produced by PowerPoint itself are generally EMF and can
        be harvested from a PPTX package that embeds such an object. PNG and JPG also work fine.

        `icon_width` and `icon_height` are `Length` values (e.g. Emu() or Inches()) that describe
        the size of the icon image within the shape. These should be omitted unless a custom
        `icon_file` is provided. The dimensions must be discovered by inspecting the XML.
        Automatic resizing of the OLE-object shape can occur when the icon is double-clicked if
        these values are not as set by PowerPoint. This behavior may only manifest in the Windows
        version of PowerPoint.
        """
        graphicFrame = _OleObjectElementCreator.graphicFrame(
            self,
            self._next_shape_id,
            object_file,
            prog_id,
            left,
            top,
            width,
            height,
            icon_file,
            icon_width,
            icon_height,
        )
        self._spTree.append(graphicFrame)
        self._recalculate_extents()
        return cast(GraphicFrame, self._shape_factory(graphicFrame))

    def add_picture(
        self,
        image_file: str | IO[bytes],
        left: Length,
        top: Length,
        width: Length | None = None,
        height: Length | None = None,
    ) -> Picture:
        """Add picture shape displaying image in `image_file`.

        `image_file` can be either a path to a file (a string) or a file-like object. The picture
        is positioned with its top-left corner at (`top`, `left`). If `width` and `height` are
        both |None|, the native size of the image is used. If only one of `width` or `height` is
        used, the unspecified dimension is calculated to preserve the aspect ratio of the image.
        If both are specified, the picture is stretched to fit, without regard to its native
        aspect ratio.
        """
        image_part, rId = self.part.get_or_add_image_part(image_file)
        pic = self._add_pic_from_image_part(image_part, rId, left, top, width, height)
        self._recalculate_extents()
        return cast(Picture, self._shape_factory(pic))

    def add_shape(
        self, autoshape_type_id: MSO_SHAPE, left: Length, top: Length, width: Length, height: Length
    ) -> Shape:
        """Return new |Shape| object appended to this shape tree.

        `autoshape_type_id` is a member of :ref:`MsoAutoShapeType` e.g. `MSO_SHAPE.RECTANGLE`
        specifying the type of shape to be added. The remaining arguments specify the new shape's
        position and size.
        """
        autoshape_type = AutoShapeType(autoshape_type_id)
        sp = self._add_sp(autoshape_type, left, top, width, height)
        self._recalculate_extents()
        return cast(Shape, self._shape_factory(sp))

    def add_textbox(self, left: Length, top: Length, width: Length, height: Length) -> Shape:
        """Return newly added text box shape appended to this shape tree.

        The text box is of the specified size, located at the specified position on the slide.
        """
        sp = self._add_textbox_sp(left, top, width, height)
        self._recalculate_extents()
        return cast(Shape, self._shape_factory(sp))

    def build_freeform(
        self, start_x: float = 0, start_y: float = 0, scale: tuple[float, float] | float = 1.0
    ) -> FreeformBuilder:
        """Return |FreeformBuilder| object to specify a freeform shape.

        The optional `start_x` and `start_y` arguments specify the starting pen position in local
        coordinates. They will be rounded to the nearest integer before use and each default to
        zero.

        The optional `scale` argument specifies the size of local coordinates proportional to
        slide coordinates (EMU). If the vertical scale is different than the horizontal scale
        (local coordinate units are "rectangular"), a pair of numeric values can be provided as
        the `scale` argument, e.g. `scale=(1.0, 2.0)`. In this case the first number is
        interpreted as the horizontal (X) scale and the second as the vertical (Y) scale.

        A convenient method for calculating scale is to divide a |Length| object by an equivalent
        count of local coordinate units, e.g. `scale = Inches(1)/1000` for 1000 local units per
        inch.
        """
        x_scale, y_scale = scale if isinstance(scale, tuple) else (scale, scale)

        return FreeformBuilder.new(self, start_x, start_y, x_scale, y_scale)

    def index(self, shape: BaseShape) -> int:
        """Return the index of `shape` in this sequence.

        Raises |ValueError| if `shape` is not in the collection.
        """
        shape_elms = list(self._element.iter_shape_elms())
        return shape_elms.index(shape.element)

    def _add_chart_graphicFrame(
        self, rId: str, x: Length, y: Length, cx: Length, cy: Length
    ) -> CT_GraphicalObjectFrame:
        """Return new `p:graphicFrame` element appended to this shape tree.

        The `p:graphicFrame` element has the specified position and size and refers to the chart
        part identified by `rId`.
        """
        shape_id = self._next_shape_id
        name = "Chart %d" % (shape_id - 1)
        graphicFrame = CT_GraphicalObjectFrame.new_chart_graphicFrame(
            shape_id, name, rId, x, y, cx, cy
        )
        self._spTree.append(graphicFrame)
        return graphicFrame

    def _add_cxnSp(
        self,
        connector_type: MSO_CONNECTOR_TYPE,
        begin_x: Length,
        begin_y: Length,
        end_x: Length,
        end_y: Length,
    ) -> CT_Connector:
        """Return a newly-added `p:cxnSp` element as specified.

        The `p:cxnSp` element is for a connector of `connector_type` beginning at (`begin_x`,
        `begin_y`) and extending to (`end_x`, `end_y`).
        """
        id_ = self._next_shape_id
        name = "Connector %d" % (id_ - 1)

        flipH, flipV = begin_x > end_x, begin_y > end_y
        x, y = min(begin_x, end_x), min(begin_y, end_y)
        cx, cy = abs(end_x - begin_x), abs(end_y - begin_y)

        return self._element.add_cxnSp(id_, name, connector_type, x, y, cx, cy, flipH, flipV)

    def _add_pic_from_image_part(
        self,
        image_part: ImagePart,
        rId: str,
        x: Length,
        y: Length,
        cx: Length | None,
        cy: Length | None,
    ) -> CT_Picture:
        """Return a newly appended `p:pic` element as specified.

        The `p:pic` element displays the image in `image_part` with size and position specified by
        `x`, `y`, `cx`, and `cy`. The element is appended to the shape tree, causing it to be
        displayed first in z-order on the slide.
        """
        id_ = self._next_shape_id
        scaled_cx, scaled_cy = image_part.scale(cx, cy)
        name = "Picture %d" % (id_ - 1)
        desc = image_part.desc
        pic = self._grpSp.add_pic(id_, name, desc, rId, x, y, scaled_cx, scaled_cy)
        return pic

    def _add_sp(
        self, autoshape_type: AutoShapeType, x: Length, y: Length, cx: Length, cy: Length
    ) -> CT_Shape:
        """Return newly-added `p:sp` element as specified.

        `p:sp` element is of `autoshape_type` at position (`x`, `y`) and of size (`cx`, `cy`).
        """
        id_ = self._next_shape_id
        name = "%s %d" % (autoshape_type.basename, id_ - 1)
        sp = self._grpSp.add_autoshape(id_, name, autoshape_type.prst, x, y, cx, cy)
        return sp

    def _add_textbox_sp(self, x: Length, y: Length, cx: Length, cy: Length) -> CT_Shape:
        """Return newly-appended textbox `p:sp` element.

        Element has position (`x`, `y`) and size (`cx`, `cy`).
        """
        id_ = self._next_shape_id
        name = "TextBox %d" % (id_ - 1)
        sp = self._spTree.add_textbox(id_, name, x, y, cx, cy)
        return sp

    def _recalculate_extents(self) -> None:
        """Adjust position and size to incorporate all contained shapes.

        This would typically be called when a contained shape is added, removed, or its position
        or size updated.
        """
        # ---default behavior is to do nothing, GroupShapes overrides to
        #    produce the distinctive behavior of groups and subgroups.---
        pass


class GroupShapes(_BaseGroupShapes):
    """The sequence of child shapes belonging to a group shape.

    Note that this collection can itself contain a group shape, making this part of a recursive,
    tree data structure (acyclic graph).
    """

    def _recalculate_extents(self) -> None:
        """Adjust position and size to incorporate all contained shapes.

        This would typically be called when a contained shape is added, removed, or its position
        or size updated.
        """
        self._grpSp.recalculate_extents()


class SlideShapes(_BaseGroupShapes):
    """Sequence of shapes appearing on a slide.

    The first shape in the sequence is the backmost in z-order and the last shape is topmost.
    Supports indexed access, len(), index(), and iteration.
    """

    parent: Slide  # pyright: ignore[reportIncompatibleMethodOverride]

    def add_movie(
        self,
        movie_file: str | IO[bytes],
        left: Length,
        top: Length,
        width: Length,
        height: Length,
        poster_frame_image: str | IO[bytes] | None = None,
        mime_type: str = CT.VIDEO,
    ) -> GraphicFrame:
        """Return newly added movie shape displaying video in `movie_file`.

        **EXPERIMENTAL.** This method has important limitations:

        * The size must be specified; no auto-scaling such as that provided by :meth:`add_picture`
          is performed.
        * The MIME type of the video file should be specified, e.g. 'video/mp4'. The provided
          video file is not interrogated for its type. The MIME type `video/unknown` is used by
          default (and works fine in tests as of this writing).
        * A poster frame image must be provided, it cannot be automatically extracted from the
          video file. If no poster frame is provided, the default "media loudspeaker" image will
          be used.

        Return a newly added movie shape to the slide, positioned at (`left`, `top`), having size
        (`width`, `height`), and containing `movie_file`. Before the video is started,
        `poster_frame_image` is displayed as a placeholder for the video.
        """
        movie_pic = _MoviePicElementCreator.new_movie_pic(
            self,
            self._next_shape_id,
            movie_file,
            left,
            top,
            width,
            height,
            poster_frame_image,
            mime_type,
        )
        self._spTree.append(movie_pic)
        self._add_video_timing(movie_pic)
        return cast(GraphicFrame, self._shape_factory(movie_pic))

    def add_table(
        self, rows: int, cols: int, left: Length, top: Length, width: Length, height: Length
    ) -> GraphicFrame:
        """Add a |GraphicFrame| object containing a table.

        The table has the specified number of `rows` and `cols` and the specified position and
        size. `width` is evenly distributed between the columns of the new table. Likewise,
        `height` is evenly distributed between the rows. Note that the `.table` property on the
        returned |GraphicFrame| shape must be used to access the enclosed |Table| object.
        """
        graphicFrame = self._add_graphicFrame_containing_table(rows, cols, left, top, width, height)
        return cast(GraphicFrame, self._shape_factory(graphicFrame))

    def clone_layout_placeholders(self, slide_layout: SlideLayout) -> None:
        """Add placeholder shapes based on those in `slide_layout`.

        Z-order of placeholders is preserved. Latent placeholders (date, slide number, and footer)
        are not cloned.
        """
        for placeholder in slide_layout.iter_cloneable_placeholders():
            self.clone_placeholder(placeholder)

    @property
    def placeholders(self) -> SlidePlaceholders:
        """Sequence of placeholder shapes in this slide."""
        return self.parent.placeholders

    @property
    def title(self) -> Shape | None:
        """The title placeholder shape on the slide.

        |None| if the slide has no title placeholder.
        """
        for elm in self._spTree.iter_ph_elms():
            if elm.ph_idx == 0:
                return cast(Shape, self._shape_factory(elm))
        return None

    def _add_graphicFrame_containing_table(
        self, rows: int, cols: int, x: Length, y: Length, cx: Length, cy: Length
    ) -> CT_GraphicalObjectFrame:
        """Return a newly added `p:graphicFrame` element containing a table as specified."""
        _id = self._next_shape_id
        name = "Table %d" % (_id - 1)
        graphicFrame = self._spTree.add_table(_id, name, rows, cols, x, y, cx, cy)
        return graphicFrame

    def _add_video_timing(self, pic: CT_Picture) -> None:
        """Add a `p:video` element under `p:sld/p:timing`.

        The element will refer to the specified `pic` element by its shape id, and cause the video
        play controls to appear for that video.
        """
        sld = self._spTree.xpath("/p:sld")[0]
        childTnLst = sld.get_or_add_childTnLst()
        childTnLst.add_video(pic.shape_id)

    def _shape_factory(self, shape_elm: ShapeElement) -> BaseShape:
        """Return an instance of the appropriate shape proxy class for `shape_elm`."""
        return SlideShapeFactory(shape_elm, self)


class LayoutShapes(_BaseShapes):
    """Sequence of shapes appearing on a slide layout.

    The first shape in the sequence is the backmost in z-order and the last shape is topmost.
    Supports indexed access, len(), index(), and iteration.
    """

    def _shape_factory(self, shape_elm: ShapeElement) -> BaseShape:
        """Return an instance of the appropriate shape proxy class for `shape_elm`."""
        return _LayoutShapeFactory(shape_elm, self)


class MasterShapes(_BaseShapes):
    """Sequence of shapes appearing on a slide master.

    The first shape in the sequence is the backmost in z-order and the last shape is topmost.
    Supports indexed access, len(), and iteration.
    """

    def _shape_factory(self, shape_elm: ShapeElement) -> BaseShape:
        """Return an instance of the appropriate shape proxy class for `shape_elm`."""
        return _MasterShapeFactory(shape_elm, self)


class NotesSlideShapes(_BaseShapes):
    """Sequence of shapes appearing on a notes slide.

    The first shape in the sequence is the backmost in z-order and the last shape is topmost.
    Supports indexed access, len(), index(), and iteration.
    """

    def ph_basename(self, ph_type: PP_PLACEHOLDER) -> str:
        """Return the base name for a placeholder of `ph_type` in this shape collection.

        A notes slide uses a different name for the body placeholder and has some unique
        placeholder types, so this method overrides the default in the base class.
        """
        return {
            PP_PLACEHOLDER.BODY: "Notes Placeholder",
            PP_PLACEHOLDER.DATE: "Date Placeholder",
            PP_PLACEHOLDER.FOOTER: "Footer Placeholder",
            PP_PLACEHOLDER.HEADER: "Header Placeholder",
            PP_PLACEHOLDER.SLIDE_IMAGE: "Slide Image Placeholder",
            PP_PLACEHOLDER.SLIDE_NUMBER: "Slide Number Placeholder",
        }[ph_type]

    def _shape_factory(self, shape_elm: ShapeElement) -> BaseShape:
        """Return appropriate shape object for `shape_elm` appearing on a notes slide."""
        return _NotesSlideShapeFactory(shape_elm, self)


class BasePlaceholders(_BaseShapes):
    """Base class for placeholder collections.

    Subclasses differentiate behaviors for a master, layout, and slide. By default, placeholder
    shapes are constructed using |BaseShapeFactory|. Subclasses should override
    :method:`_shape_factory` to use custom placeholder classes.
    """

    @staticmethod
    def _is_member_elm(shape_elm: ShapeElement) -> bool:
        """True if `shape_elm` is a placeholder shape, False otherwise."""
        return shape_elm.has_ph_elm


class LayoutPlaceholders(BasePlaceholders):
    """Sequence of |LayoutPlaceholder| instance for each placeholder shape on a slide layout."""

    __iter__: Callable[  # pyright: ignore[reportIncompatibleMethodOverride]
        [], Iterator[LayoutPlaceholder]
    ]

    def get(self, idx: int, default: LayoutPlaceholder | None = None) -> LayoutPlaceholder | None:
        """The first placeholder shape with matching `idx` value, or `default` if not found."""
        for placeholder in self:
            if placeholder.element.ph_idx == idx:
                return placeholder
        return default

    def _shape_factory(self, shape_elm: ShapeElement) -> BaseShape:
        """Return an instance of the appropriate shape proxy class for `shape_elm`."""
        return _LayoutShapeFactory(shape_elm, self)


class MasterPlaceholders(BasePlaceholders):
    """Sequence of MasterPlaceholder representing the placeholder shapes on a slide master."""

    __iter__: Callable[  # pyright: ignore[reportIncompatibleMethodOverride]
        [], Iterator[MasterPlaceholder]
    ]

    def get(self, ph_type: PP_PLACEHOLDER, default: MasterPlaceholder | None = None):
        """Return the first placeholder shape with type `ph_type` (e.g. 'body').

        Returns `default` if no such placeholder shape is present in the collection.
        """
        for placeholder in self:
            if placeholder.ph_type == ph_type:
                return placeholder
        return default

    def _shape_factory(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, placeholder_elm: CT_Shape
    ) -> MasterPlaceholder:
        """Return an instance of the appropriate shape proxy class for `shape_elm`."""
        return cast(MasterPlaceholder, _MasterShapeFactory(placeholder_elm, self))


class NotesSlidePlaceholders(MasterPlaceholders):
    """Sequence of placeholder shapes on a notes slide."""

    __iter__: Callable[  # pyright: ignore[reportIncompatibleMethodOverride]
        [], Iterator[NotesSlidePlaceholder]
    ]

    def _shape_factory(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, placeholder_elm: CT_Shape
    ) -> NotesSlidePlaceholder:
        """Return an instance of the appropriate placeholder proxy class for `placeholder_elm`."""
        return cast(NotesSlidePlaceholder, _NotesSlideShapeFactory(placeholder_elm, self))


class SlidePlaceholders(ParentedElementProxy):
    """Collection of placeholder shapes on a slide.

    Supports iteration, :func:`len`, and dictionary-style lookup on the `idx` value of the
    placeholders it contains.
    """

    _element: CT_GroupShape

    def __getitem__(self, idx: int):
        """Access placeholder shape having `idx`.

        Note that while this looks like list access, idx is actually a dictionary key and will
        raise |KeyError| if no placeholder with that idx value is in the collection.
        """
        for e in self._element.iter_ph_elms():
            if e.ph_idx == idx:
                return SlideShapeFactory(e, self)
        raise KeyError("no placeholder on this slide with idx == %d" % idx)

    def __iter__(self):
        """Generate placeholder shapes in `idx` order."""
        ph_elms = sorted([e for e in self._element.iter_ph_elms()], key=lambda e: e.ph_idx)
        return (SlideShapeFactory(e, self) for e in ph_elms)

    def __len__(self) -> int:
        """Return count of placeholder shapes."""
        return len(list(self._element.iter_ph_elms()))


def BaseShapeFactory(shape_elm: ShapeElement, parent: ProvidesPart) -> BaseShape:
    """Return an instance of the appropriate shape proxy class for `shape_elm`."""
    tag = shape_elm.tag

    if isinstance(shape_elm, CT_Picture):
        videoFiles = shape_elm.xpath("./p:nvPicPr/p:nvPr/a:videoFile")
        if videoFiles:
            return Movie(shape_elm, parent)
        return Picture(shape_elm, parent)

    shape_cls = {
        qn("p:cxnSp"): Connector,
        qn("p:grpSp"): GroupShape,
        qn("p:sp"): Shape,
        qn("p:graphicFrame"): GraphicFrame,
    }.get(tag, BaseShape)

    return shape_cls(shape_elm, parent)  # pyright: ignore[reportArgumentType]


def _LayoutShapeFactory(shape_elm: ShapeElement, parent: ProvidesPart) -> BaseShape:
    """Return appropriate shape object for `shape_elm` on a slide layout."""
    if isinstance(shape_elm, CT_Shape) and shape_elm.has_ph_elm:
        return LayoutPlaceholder(shape_elm, parent)
    return BaseShapeFactory(shape_elm, parent)


def _MasterShapeFactory(shape_elm: ShapeElement, parent: ProvidesPart) -> BaseShape:
    """Return appropriate shape object for `shape_elm` on a slide master."""
    if isinstance(shape_elm, CT_Shape) and shape_elm.has_ph_elm:
        return MasterPlaceholder(shape_elm, parent)
    return BaseShapeFactory(shape_elm, parent)


def _NotesSlideShapeFactory(shape_elm: ShapeElement, parent: ProvidesPart) -> BaseShape:
    """Return appropriate shape object for `shape_elm` on a notes slide."""
    if isinstance(shape_elm, CT_Shape) and shape_elm.has_ph_elm:
        return NotesSlidePlaceholder(shape_elm, parent)
    return BaseShapeFactory(shape_elm, parent)


def _SlidePlaceholderFactory(shape_elm: ShapeElement, parent: ProvidesPart):
    """Return a placeholder shape of the appropriate type for `shape_elm`."""
    tag = shape_elm.tag
    if tag == qn("p:sp"):
        Constructor = {
            PP_PLACEHOLDER.BITMAP: PicturePlaceholder,
            PP_PLACEHOLDER.CHART: ChartPlaceholder,
            PP_PLACEHOLDER.PICTURE: PicturePlaceholder,
            PP_PLACEHOLDER.TABLE: TablePlaceholder,
        }.get(shape_elm.ph_type, SlidePlaceholder)
    elif tag == qn("p:graphicFrame"):
        Constructor = PlaceholderGraphicFrame
    elif tag == qn("p:pic"):
        Constructor = PlaceholderPicture
    else:
        Constructor = BaseShapeFactory
    return Constructor(shape_elm, parent)  # pyright: ignore[reportArgumentType]


def SlideShapeFactory(shape_elm: ShapeElement, parent: ProvidesPart) -> BaseShape:
    """Return appropriate shape object for `shape_elm` on a slide."""
    if shape_elm.has_ph_elm:
        return _SlidePlaceholderFactory(shape_elm, parent)
    return BaseShapeFactory(shape_elm, parent)


class _MoviePicElementCreator(object):
    """Functional service object for creating a new movie p:pic element.

    It's entire external interface is its :meth:`new_movie_pic` class method that returns a new
    `p:pic` element containing the specified video. This class is not intended to be constructed
    or an instance of it retained by the caller; it is a "one-shot" object, really a function
    wrapped in a object such that its helper methods can be organized here.
    """

    def __init__(
        self,
        shapes: SlideShapes,
        shape_id: int,
        movie_file: str | IO[bytes],
        x: Length,
        y: Length,
        cx: Length,
        cy: Length,
        poster_frame_file: str | IO[bytes] | None,
        mime_type: str | None,
    ):
        super(_MoviePicElementCreator, self).__init__()
        self._shapes = shapes
        self._shape_id = shape_id
        self._movie_file = movie_file
        self._x, self._y, self._cx, self._cy = x, y, cx, cy
        self._poster_frame_file = poster_frame_file
        self._mime_type = mime_type

    @classmethod
    def new_movie_pic(
        cls,
        shapes: SlideShapes,
        shape_id: int,
        movie_file: str | IO[bytes],
        x: Length,
        y: Length,
        cx: Length,
        cy: Length,
        poster_frame_image: str | IO[bytes] | None,
        mime_type: str | None,
    ) -> CT_Picture:
        """Return a new `p:pic` element containing video in `movie_file`.

        If `mime_type` is None, 'video/unknown' is used. If `poster_frame_file` is None, the
        default "media loudspeaker" image is used.
        """
        return cls(shapes, shape_id, movie_file, x, y, cx, cy, poster_frame_image, mime_type)._pic

    @property
    def _media_rId(self) -> str:
        """Return the rId of RT.MEDIA relationship to video part.

        For historical reasons, there are two relationships to the same part; one is the video rId
        and the other is the media rId.
        """
        return self._video_part_rIds[0]

    @lazyproperty
    def _pic(self) -> CT_Picture:
        """Return the new `p:pic` element referencing the video."""
        return CT_Picture.new_video_pic(
            self._shape_id,
            self._shape_name,
            self._video_rId,
            self._media_rId,
            self._poster_frame_rId,
            self._x,
            self._y,
            self._cx,
            self._cy,
        )

    @lazyproperty
    def _poster_frame_image_file(self) -> str | IO[bytes]:
        """Return the image file for video placeholder image.

        If no poster frame file is provided, the default "media loudspeaker" image is used.
        """
        poster_frame_file = self._poster_frame_file
        if poster_frame_file is None:
            return io.BytesIO(SPEAKER_IMAGE_BYTES)
        return poster_frame_file

    @lazyproperty
    def _poster_frame_rId(self) -> str:
        """Return the rId of relationship to poster frame image.

        The poster frame is the image used to represent the video before it's played.
        """
        _, poster_frame_rId = self._slide_part.get_or_add_image_part(self._poster_frame_image_file)
        return poster_frame_rId

    @property
    def _shape_name(self) -> str:
        """Return the appropriate shape name for the p:pic shape.

        A movie shape is named with the base filename of the video.
        """
        return self._video.filename

    @property
    def _slide_part(self) -> SlidePart:
        """Return SlidePart object for slide containing this movie."""
        return self._shapes.part

    @lazyproperty
    def _video(self) -> Video:
        """Return a |Video| object containing the movie file."""
        return Video.from_path_or_file_like(self._movie_file, self._mime_type)

    @lazyproperty
    def _video_part_rIds(self) -> tuple[str, str]:
        """Return the rIds for relationships to media part for video.

        This is where the media part and its relationships to the slide are actually created.
        """
        media_rId, video_rId = self._slide_part.get_or_add_video_media_part(self._video)
        return media_rId, video_rId

    @property
    def _video_rId(self) -> str:
        """Return the rId of RT.VIDEO relationship to video part.

        For historical reasons, there are two relationships to the same part; one is the video rId
        and the other is the media rId.
        """
        return self._video_part_rIds[1]


class _OleObjectElementCreator(object):
    """Functional service object for creating a new OLE-object p:graphicFrame element.

    It's entire external interface is its :meth:`graphicFrame` class method that returns a new
    `p:graphicFrame` element containing the specified embedded OLE-object shape. This class is not
    intended to be constructed or an instance of it retained by the caller; it is a "one-shot"
    object, really a function wrapped in a object such that its helper methods can be organized
    here.
    """

    def __init__(
        self,
        shapes: _BaseGroupShapes,
        shape_id: int,
        ole_object_file: str | IO[bytes],
        prog_id: PROG_ID | str,
        x: Length,
        y: Length,
        cx: Length | None,
        cy: Length | None,
        icon_file: str | IO[bytes] | None,
        icon_width: Length | None,
        icon_height: Length | None,
    ):
        self._shapes = shapes
        self._shape_id = shape_id
        self._ole_object_file = ole_object_file
        self._prog_id_arg = prog_id
        self._x = x
        self._y = y
        self._cx_arg = cx
        self._cy_arg = cy
        self._icon_file_arg = icon_file
        self._icon_width_arg = icon_width
        self._icon_height_arg = icon_height

    @classmethod
    def graphicFrame(
        cls,
        shapes: _BaseGroupShapes,
        shape_id: int,
        ole_object_file: str | IO[bytes],
        prog_id: PROG_ID | str,
        x: Length,
        y: Length,
        cx: Length | None,
        cy: Length | None,
        icon_file: str | IO[bytes] | None,
        icon_width: Length | None,
        icon_height: Length | None,
    ) -> CT_GraphicalObjectFrame:
        """Return new `p:graphicFrame` element containing embedded `ole_object_file`."""
        return cls(
            shapes,
            shape_id,
            ole_object_file,
            prog_id,
            x,
            y,
            cx,
            cy,
            icon_file,
            icon_width,
            icon_height,
        )._graphicFrame

    @lazyproperty
    def _graphicFrame(self) -> CT_GraphicalObjectFrame:
        """Newly-created `p:graphicFrame` element referencing embedded OLE-object."""
        return CT_GraphicalObjectFrame.new_ole_object_graphicFrame(
            self._shape_id,
            self._shape_name,
            self._ole_object_rId,
            self._progId,
            self._icon_rId,
            self._x,
            self._y,
            self._cx,
            self._cy,
            self._icon_width,
            self._icon_height,
        )

    @lazyproperty
    def _cx(self) -> Length:
        """Emu object specifying width of "show-as-icon" image for OLE shape."""
        # --- a user-specified width overrides any default ---
        if self._cx_arg is not None:
            return self._cx_arg

        # --- the default width is specified by the PROG_ID member if prog_id is one,
        # --- otherwise it gets the default icon width.
        return (
            Emu(self._prog_id_arg.width) if isinstance(self._prog_id_arg, PROG_ID) else Emu(965200)
        )

    @lazyproperty
    def _cy(self) -> Length:
        """Emu object specifying height of "show-as-icon" image for OLE shape."""
        # --- a user-specified width overrides any default ---
        if self._cy_arg is not None:
            return self._cy_arg

        # --- the default height is specified by the PROG_ID member if prog_id is one,
        # --- otherwise it gets the default icon height.
        return (
            Emu(self._prog_id_arg.height) if isinstance(self._prog_id_arg, PROG_ID) else Emu(609600)
        )

    @lazyproperty
    def _icon_height(self) -> Length:
        """Vertical size of enclosed EMF icon within the OLE graphic-frame.

        This must be specified when a custom icon is used, to avoid stretching of the image and
        possible undesired resizing by PowerPoint when the OLE shape is double-clicked to open it.

        The correct size can be determined by creating an example PPTX using PowerPoint and then
        inspecting the XML of the OLE graphics-frame (p:oleObj.imgH).
        """
        return self._icon_height_arg if self._icon_height_arg is not None else Emu(609600)

    @lazyproperty
    def _icon_image_file(self) -> str | IO[bytes]:
        """Reference to image file containing icon to show in lieu of this object.

        This can be either a str path or a file-like object (io.BytesIO typically).
        """
        # --- a user-specified icon overrides any default ---
        if self._icon_file_arg is not None:
            return self._icon_file_arg

        # --- A prog_id belonging to PROG_ID gets its icon filename from there. A
        # --- user-specified (str) prog_id gets the default icon.
        icon_filename = (
            self._prog_id_arg.icon_filename
            if isinstance(self._prog_id_arg, PROG_ID)
            else "generic-icon.emf"
        )

        import importlib.resources
        return (importlib.resources.files("pptx") / "templates" / icon_filename).open(mode="rb")

    @lazyproperty
    def _icon_rId(self) -> str:
        """str rId like "rId7" of rel to icon (image) representing OLE-object part."""
        _, rId = self._slide_part.get_or_add_image_part(self._icon_image_file)
        return rId

    @lazyproperty
    def _icon_width(self) -> Length:
        """Width of enclosed EMF icon within the OLE graphic-frame.

        This must be specified when a custom icon is used, to avoid stretching of the image and
        possible undesired resizing by PowerPoint when the OLE shape is double-clicked to open it.
        """
        return self._icon_width_arg if self._icon_width_arg is not None else Emu(965200)

    @lazyproperty
    def _ole_object_rId(self) -> str:
        """str rId like "rId6" of relationship to embedded ole_object part.

        This is where the ole_object part and its relationship to the slide are actually created.
        """
        return self._slide_part.add_embedded_ole_object_part(
            self._prog_id_arg, self._ole_object_file
        )

    @lazyproperty
    def _progId(self) -> str:
        """str like "Excel.Sheet.12" identifying program used to open object.

        This value appears in the `progId` attribute of the `p:oleObj` element for the object.
        """
        prog_id_arg = self._prog_id_arg

        # --- member of PROG_ID enumeration knows its progId keyphrase, otherwise caller
        # --- has specified it explicitly (as str)
        return prog_id_arg.progId if isinstance(prog_id_arg, PROG_ID) else prog_id_arg

    @lazyproperty
    def _shape_name(self) -> str:
        """str name like "Object 1" for the embedded ole_object shape.

        The name is formed from the prefix "Object " and the shape-id decremented by 1.
        """
        return "Object %d" % (self._shape_id - 1)

    @lazyproperty
    def _slide_part(self) -> SlidePart:
        """SlidePart object for this slide."""
        return self._shapes.part
