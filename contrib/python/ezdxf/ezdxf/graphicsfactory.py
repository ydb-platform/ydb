# Copyright (c) 2013-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Sequence,
    cast,
    Optional,
)
import math
import logging
import warnings
from ezdxf.lldxf import const
from ezdxf.lldxf.const import DXFValueError, DXFVersionError, DXF2000, DXF2013
from ezdxf.math import (
    Vec3,
    UVec,
    UCS,
    global_bspline_interpolation,
    fit_points_to_cad_cv,
    arc_angle_span_deg,
    ConstructionArc,
    NULLVEC,
)
from ezdxf.render.arrows import ARROWS
from ezdxf.entities import factory, Point, Spline, Body, Surface, Line, Circle
from ezdxf.entities.mtext_columns import *
from ezdxf.entities.dimstyleoverride import DimStyleOverride
from ezdxf.render.dim_linear import multi_point_linear_dimension
from ezdxf.tools import guid

logger = logging.getLogger("ezdxf")

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.eztypes import GenericLayoutType
    from ezdxf.entities import (
        Arc,
        AttDef,
        Dimension,
        ArcDimension,
        DXFGraphic,
        Ellipse,
        ExtrudedSurface,
        Face3d,
        Hatch,
        Image,
        ImageDef,
        Insert,
        LWPolyline,
        Leader,
        LoftedSurface,
        MLine,
        MText,
        MPolygon,
        Mesh,
        Polyface,
        Polyline,
        Polymesh,
        Ray,
        Region,
        RevolvedSurface,
        Shape,
        Solid,
        Solid3d,
        SweptSurface,
        Text,
        Trace,
        Underlay,
        UnderlayDefinition,
        Wipeout,
        XLine,
        MultiLeader,
        Helix,
    )
    from ezdxf.render.mleader import (
        MultiLeaderMTextBuilder,
        MultiLeaderBlockBuilder,
    )


class CreatorInterface:
    def __init__(self, doc: Drawing):
        self.doc = doc

    @property
    def dxfversion(self) -> str:
        return self.doc.dxfversion

    @property
    def is_active_paperspace(self):
        return False

    def new_entity(self, type_: str, dxfattribs: dict) -> DXFGraphic:
        """
        Create entity in drawing database and add entity to the entity space.

        Args:
            type_ : DXF type string, like "LINE", "CIRCLE" or "LWPOLYLINE"
            dxfattribs: DXF attributes for the new entity

        """
        entity = factory.create_db_entry(type_, dxfattribs, self.doc)
        self.add_entity(entity)  # type: ignore
        return entity  # type: ignore

    def add_entity(self, entity: DXFGraphic) -> None:
        pass

    def add_point(self, location: UVec, dxfattribs=None) -> Point:
        """
        Add a :class:`~ezdxf.entities.Point` entity at `location`.

        Args:
            location: 2D/3D point in :ref:`WCS`
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["location"] = Vec3(location)
        return self.new_entity("POINT", dxfattribs)  # type: ignore

    def add_line(self, start: UVec, end: UVec, dxfattribs=None) -> Line:
        """
        Add a :class:`~ezdxf.entities.Line` entity from `start` to `end`.

        Args:
            start: 2D/3D point in :ref:`WCS`
            end: 2D/3D point in :ref:`WCS`
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["start"] = Vec3(start)
        dxfattribs["end"] = Vec3(end)
        return self.new_entity("LINE", dxfattribs)  # type: ignore

    def add_circle(
        self, center: UVec, radius: float, dxfattribs=None
    ) -> Circle:
        """
        Add a :class:`~ezdxf.entities.Circle` entity. This is an 2D element,
        which can be placed in space by using :ref:`OCS`.

        Args:
            center: 2D/3D point in :ref:`WCS`
            radius: circle radius
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["center"] = Vec3(center)
        dxfattribs["radius"] = float(radius)
        return self.new_entity("CIRCLE", dxfattribs)  # type: ignore

    def add_ellipse(
        self,
        center: UVec,
        major_axis: UVec = (1, 0, 0),
        ratio: float = 1,
        start_param: float = 0,
        end_param: float = math.tau,
        dxfattribs=None,
    ) -> Ellipse:
        """
        Add an :class:`~ezdxf.entities.Ellipse` entity, `ratio` is the ratio of
        minor axis to major axis, `start_param` and `end_param` defines start
        and end point of the ellipse, a full ellipse goes from 0 to 2π.
        The ellipse goes from start to end param in `counter-clockwise`
        direction.

        Args:
            center: center of ellipse as 2D/3D point in :ref:`WCS`
            major_axis: major axis as vector (x, y, z)
            ratio: ratio of minor axis to major axis in range +/-[1e-6, 1.0]
            start_param: start of ellipse curve
            end_param: end param of ellipse curve
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("ELLIPSE requires DXF R2000")
        ratio = float(ratio)
        if abs(ratio) > 1.0:  # not valid for AutoCAD
            raise DXFValueError("invalid axis ratio > 1.0")
        _major_axis = Vec3(major_axis)
        if _major_axis.is_null:  # not valid for AutoCAD
            raise DXFValueError("invalid major axis: (0, 0, 0)")
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["center"] = Vec3(center)
        dxfattribs["major_axis"] = _major_axis
        dxfattribs["ratio"] = ratio
        dxfattribs["start_param"] = float(start_param)
        dxfattribs["end_param"] = float(end_param)
        return self.new_entity("ELLIPSE", dxfattribs)  # type: ignore

    def add_arc(
        self,
        center: UVec,
        radius: float,
        start_angle: float,
        end_angle: float,
        is_counter_clockwise: bool = True,
        dxfattribs=None,
    ) -> Arc:
        """
        Add an :class:`~ezdxf.entities.Arc` entity. The arc goes from
        `start_angle` to `end_angle` in counter-clockwise direction by default,
        set parameter `is_counter_clockwise` to ``False`` for clockwise
        orientation.

        Args:
            center: center of arc as 2D/3D point in :ref:`WCS`
            radius: arc radius
            start_angle: start angle in degrees
            end_angle: end angle in degrees
            is_counter_clockwise: ``False`` for clockwise orientation
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["center"] = Vec3(center)
        dxfattribs["radius"] = float(radius)
        if is_counter_clockwise:
            dxfattribs["start_angle"] = float(start_angle)
            dxfattribs["end_angle"] = float(end_angle)
        else:
            dxfattribs["start_angle"] = float(end_angle)
            dxfattribs["end_angle"] = float(start_angle)
        return self.new_entity("ARC", dxfattribs)  # type: ignore

    def add_solid(self, points: Iterable[UVec], dxfattribs=None) -> Solid:
        """Add a :class:`~ezdxf.entities.Solid` entity, `points` is an iterable
        of 3 or 4 points.

        .. hint::

                The last two vertices are in reversed order: a square has the
                vertex order 0-1-3-2

        Args:
            points: iterable of 3 or 4 2D/3D points in :ref:`WCS`
            dxfattribs: additional DXF attributes

        """
        return self._add_quadrilateral("SOLID", points, dxfattribs)  # type: ignore

    def add_trace(self, points: Iterable[UVec], dxfattribs=None) -> Trace:
        """Add a :class:`~ezdxf.entities.Trace` entity, `points` is an iterable
        of 3 or 4 points.

        .. hint::

                The last two vertices are in reversed order: a square has the
                vertex order 0-1-3-2

        Args:
            points: iterable of 3 or 4 2D/3D points in :ref:`WCS`
            dxfattribs: additional DXF attributes

        """
        return self._add_quadrilateral("TRACE", points, dxfattribs)  # type: ignore

    def add_3dface(self, points: Iterable[UVec], dxfattribs=None) -> Face3d:
        """
        Add a :class:`~ezdxf.entities.3DFace` entity, `points` is an iterable
        3 or 4 2D/3D points.

        .. hint::

                In contrast to SOLID and TRACE, the last two vertices are in
                regular order: a square has the vertex order 0-1-2-3

        Args:
            points: iterable of 3 or 4 2D/3D points in :ref:`WCS`
            dxfattribs: additional DXF attributes

        """
        return self._add_quadrilateral("3DFACE", points, dxfattribs)  # type: ignore

    def add_text(
        self,
        text: str,
        *,
        height: Optional[float] = None,
        rotation: Optional[float] = None,
        dxfattribs=None,
    ) -> Text:
        """
        Add a :class:`~ezdxf.entities.Text` entity, see also
        :class:`~ezdxf.entities.Textstyle`.

        Args:
            text: content string
            height: text height in drawing units
            rotation: text rotation in degrees
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["text"] = str(text)
        if height is not None:
            dxfattribs["height"] = float(height)
        if rotation is not None:
            dxfattribs["rotation"] = float(rotation)
        dxfattribs.setdefault("insert", Vec3())
        return self.new_entity("TEXT", dxfattribs)  # type: ignore

    def add_blockref(self, name: str, insert: UVec, dxfattribs=None) -> Insert:
        """
        Add an :class:`~ezdxf.entities.Insert` entity.

        When inserting a block reference into the modelspace or another block
        layout with different units, the scaling factor between these units
        should be applied as scaling attributes (:attr:`xscale`, ...) e.g.
        modelspace in meters and block in centimeters, :attr:`xscale` has to
        be 0.01.

        Args:
            name: block name as str
            insert: insert location as 2D/3D point in :ref:`WCS`
            dxfattribs: additional DXF attributes

        """
        if not isinstance(name, str):
            raise DXFValueError("Block name as string required.")

        dxfattribs = dict(dxfattribs or {})
        dxfattribs["name"] = name
        dxfattribs["insert"] = Vec3(insert)
        return self.new_entity("INSERT", dxfattribs)  # type: ignore

    def add_auto_blockref(
        self,
        name: str,
        insert: UVec,
        values: dict[str, str],
        dxfattribs=None,
    ) -> Insert:
        """
        Add an :class:`~ezdxf.entities.Insert` entity. This method adds for each
        :class:`~ezdxf.entities.Attdef` entity, defined in the block definition,
        automatically an :class:`Attrib` entity to the block reference and set
        (tag, value) DXF attributes of the ATTRIB entities by the (key, value)
        pairs (both as strings) of the `values` dict.

        The Attrib entities are placed relative to the insert point, which is
        equal to the block base point.

        This method wraps the INSERT and all the ATTRIB entities into an
        anonymous block, which produces the best visual results, especially for
        non-uniform scaled block references, because the transformation and
        scaling is done by the CAD application. But this makes evaluation of
        block references with attributes more complicated, if you prefer INSERT
        and ATTRIB entities without a wrapper block use the
        :meth:`add_blockref_with_attribs` method.

        Args:
            name: block name
            insert: insert location as 2D/3D point in :ref:`WCS`
            values: :class:`~ezdxf.entities.Attrib` tag values as (tag, value) pairs
            dxfattribs: additional DXF attributes

        """
        if not isinstance(name, str):
            raise DXFValueError("Block name as string required.")

        def unpack(dxfattribs) -> tuple[str, str, UVec]:
            tag = dxfattribs.pop("tag")
            text = values.get(tag, "")
            location = dxfattribs.pop("insert")
            return tag, text, location

        def autofill() -> None:
            # ATTRIBs are placed relative to the base point
            for attdef in blockdef.attdefs():
                dxfattribs = attdef.dxfattribs(drop={"prompt", "handle"})
                tag, text, location = unpack(dxfattribs)
                blockref.add_attrib(tag, text, location, dxfattribs)

        dxfattribs = dict(dxfattribs or {})
        autoblock = self.doc.blocks.new_anonymous_block()
        blockref = autoblock.add_blockref(name, (0, 0))
        blockdef = self.doc.blocks[name]
        autofill()
        return self.add_blockref(autoblock.name, insert, dxfattribs)

    def add_attdef(
        self,
        tag: str,
        insert: UVec = (0, 0),
        text: str = "",
        *,
        height: Optional[float] = None,
        rotation: Optional[float] = None,
        dxfattribs=None,
    ) -> AttDef:
        """
        Add an :class:`~ezdxf.entities.AttDef` as stand alone DXF entity.

        Set position and alignment by the idiom::

            layout.add_attdef("NAME").set_placement(
                (2, 3), align=TextEntityAlignment.MIDDLE_CENTER
            )

        Args:
            tag: tag name as string
            insert: insert location as 2D/3D point in :ref:`WCS`
            text: tag value as string
            height: text height in drawing units
            rotation: text rotation in degrees
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["tag"] = str(tag)
        dxfattribs["insert"] = Vec3(insert)
        dxfattribs["text"] = str(text)
        if height is not None:
            dxfattribs["height"] = float(height)
        if rotation is not None:
            dxfattribs["rotation"] = float(rotation)
        return self.new_entity("ATTDEF", dxfattribs)  # type: ignore

    def add_polyline2d(
        self,
        points: Iterable[UVec],
        format: Optional[str] = None,
        *,
        close: bool = False,
        dxfattribs=None,
    ) -> Polyline:
        """
        Add a 2D :class:`~ezdxf.entities.Polyline` entity.

        Args:
            points: iterable of 2D points in :ref:`WCS`
            close: ``True`` for a closed polyline
            format: user defined point format like :meth:`add_lwpolyline`,
                default is ``None``
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        if "closed" in dxfattribs:
            warnings.warn(
                'dxfattribs key "closed" is deprecated, '
                'use keyword argument "close"',
                DeprecationWarning,
            )

        close = dxfattribs.pop("closed", close)
        polyline: Polyline = self.new_entity("POLYLINE", dxfattribs)  # type: ignore
        polyline.close(close)
        if format is not None:
            polyline.append_formatted_vertices(points, format=format)
        else:
            polyline.append_vertices(points)
        if self.doc:
            polyline.add_sub_entities_to_entitydb(self.doc.entitydb)
        return polyline

    def add_polyline3d(
        self,
        points: Iterable[UVec],
        *,
        close: bool = False,
        dxfattribs=None,
    ) -> Polyline:
        """Add a 3D :class:`~ezdxf.entities.Polyline` entity.

        Args:
            points: iterable of 3D points in :ref:`WCS`
            close: ``True`` for a closed polyline
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["flags"] = (
            dxfattribs.get("flags", 0) | const.POLYLINE_3D_POLYLINE
        )
        return self.add_polyline2d(points, close=close, dxfattribs=dxfattribs)

    def add_polymesh(
        self, size: tuple[int, int] = (3, 3), dxfattribs=None
    ) -> Polymesh:
        """
        Add a :class:`~ezdxf.entities.Polymesh` entity, which is a wrapper class
        for the POLYLINE entity. A polymesh is a grid of `mcount` x `ncount`
        vertices and every vertex has its own (x, y, z)-coordinates.

        Args:
            size: 2-tuple (`mcount`, `ncount`)
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["flags"] = (
            dxfattribs.get("flags", 0) | const.POLYLINE_3D_POLYMESH
        )
        m_size = max(size[0], 2)
        n_size = max(size[1], 2)
        dxfattribs["m_count"] = m_size
        dxfattribs["n_count"] = n_size
        m_close = dxfattribs.pop("m_close", False)
        n_close = dxfattribs.pop("n_close", False)
        polymesh: Polymesh = self.new_entity("POLYLINE", dxfattribs)  # type: ignore

        points = [(0, 0, 0)] * (m_size * n_size)
        polymesh.append_vertices(points)  # init mesh vertices
        polymesh.close(m_close, n_close)
        if self.doc:
            polymesh.add_sub_entities_to_entitydb(self.doc.entitydb)

        return polymesh

    def add_polyface(self, dxfattribs=None) -> Polyface:
        """Add a :class:`~ezdxf.entities.Polyface` entity, which is a wrapper
        class for the POLYLINE entity.

        Args:
            dxfattribs: additional DXF attributes for
                :class:`~ezdxf.entities.Polyline` entity

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["flags"] = (
            dxfattribs.get("flags", 0) | const.POLYLINE_POLYFACE
        )
        m_close = dxfattribs.pop("m_close", False)
        n_close = dxfattribs.pop("n_close", False)
        polyface: Polyface = self.new_entity("POLYLINE", dxfattribs)  # type: ignore
        polyface.close(m_close, n_close)
        if self.doc:
            polyface.add_sub_entities_to_entitydb(self.doc.entitydb)

        return polyface

    def _add_quadrilateral(
        self, type_: str, points: Iterable[UVec], dxfattribs=None
    ) -> DXFGraphic:
        dxfattribs = dict(dxfattribs or {})
        entity = self.new_entity(type_, dxfattribs)
        for x, point in enumerate(self._four_points(points)):
            entity[x] = Vec3(point)  # type: ignore
        return entity

    @staticmethod
    def _four_points(points: Iterable[UVec]) -> Iterable[UVec]:
        vertices = list(points)
        if len(vertices) not in (3, 4):
            raise DXFValueError("3 or 4 points required.")
        for vertex in vertices:
            yield vertex
        if len(vertices) == 3:
            yield vertices[-1]  # last again

    def add_shape(
        self,
        name: str,
        insert: UVec = (0, 0),
        size: float = 1.0,
        dxfattribs=None,
    ) -> Shape:
        """
        Add a :class:`~ezdxf.entities.Shape` reference to an external stored shape.

        Args:
            name: shape name as string
            insert: insert location as 2D/3D point in :ref:`WCS`
            size: size factor
            dxfattribs: additional DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["name"] = str(name)
        dxfattribs["insert"] = Vec3(insert)
        dxfattribs["size"] = float(size)
        return self.new_entity("SHAPE", dxfattribs)  # type: ignore

    # new entities in DXF AC1015 (R2000)

    def add_lwpolyline(
        self,
        points: Iterable[UVec],
        format: str = "xyseb",
        *,
        close: bool = False,
        dxfattribs=None,
    ) -> LWPolyline:
        """
        Add a 2D polyline as :class:`~ezdxf.entities.LWPolyline` entity.
        A points are defined as (x, y, [start_width, [end_width, [bulge]]])
        tuples, but order can be redefined by the `format` argument. Set
        `start_width`, `end_width` to 0 to be ignored like
        (x, y, 0, 0, bulge).

        The :class:`~ezdxf.entities.LWPolyline` is defined as a single DXF
        entity and needs less disk space than a
        :class:`~ezdxf.entities.Polyline` entity. (requires DXF R2000)

        Format codes:

            - ``x`` = x-coordinate
            - ``y`` = y-coordinate
            - ``s`` = start width
            - ``e`` = end width
            - ``b`` = bulge value
            - ``v`` = (x, y [,z]) tuple (z-axis is ignored)

        Args:
            points: iterable of (x, y, [start_width, [end_width, [bulge]]]) tuples
            format: user defined point format, default is "xyseb"
            close: ``True`` for a closed polyline
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("LWPOLYLINE requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        if "closed" in dxfattribs:
            warnings.warn(
                'dxfattribs key "closed" is deprecated, '
                'use keyword argument "close"',
                DeprecationWarning,
            )
        close = dxfattribs.pop("closed", close)
        lwpolyline: LWPolyline = self.new_entity("LWPOLYLINE", dxfattribs)  # type: ignore
        lwpolyline.set_points(points, format=format)
        lwpolyline.closed = close
        return lwpolyline

    def add_mtext(self, text: str, dxfattribs=None) -> MText:
        """
        Add a multiline text entity with automatic text wrapping at boundaries
        as :class:`~ezdxf.entities.MText` entity.
        (requires DXF R2000)

        Args:
            text: content string
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("MTEXT requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        mtext: MText = self.new_entity("MTEXT", dxfattribs)  # type: ignore
        mtext.text = str(text)
        return mtext

    def add_mtext_static_columns(
        self,
        content: Iterable[str],
        width: float,
        gutter_width: float,
        height: float,
        dxfattribs=None,
    ) -> MText:
        """Add a multiline text entity with static columns as
        :class:`~ezdxf.entities.MText` entity. The content is spread
        across the columns, the count of content strings determine the count
        of columns.

        This factory method adds automatically a column break ``"\\N"`` at the
        end of each column text to force a new column.
        The `height` attribute should be big enough to reserve enough space for
        the tallest column. Too small values produce valid DXF files, but the
        visual result will not be as expected. The `height` attribute also
        defines the total height of the MTEXT entity.

        (requires DXF R2000)

        Args:
            content: iterable of column content
            width: column width
            gutter_width: distance between columns
            height: max. column height
            dxfattribs: additional DXF attributes

        """
        dxfversion = self.dxfversion
        if dxfversion < DXF2000:
            raise DXFVersionError("MTEXT requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        content = list(content)
        if dxfversion < const.DXF2018:
            mtext = make_static_columns_r2000(
                content, width, gutter_width, height, dxfattribs
            )
        else:
            mtext = make_static_columns_r2018(
                content, width, gutter_width, height, dxfattribs
            )
        if self.doc:
            self.doc.entitydb.add(mtext)
        self.add_entity(mtext)
        return mtext

    def add_mtext_dynamic_auto_height_columns(
        self,
        content: str,
        width: float,
        gutter_width: float,
        height: float,
        count: int,
        dxfattribs=None,
    ) -> MText:
        """Add a multiline text entity with as many columns as needed for the
        given common fixed `height`. The content is spread across the columns
        automatically by the CAD application. The `height` argument also defines
        the total height of the MTEXT entity. To get the correct column `count`
        requires an **exact** MTEXT rendering like AutoCAD, which is
        not done by `ezdxf`, therefore passing the expected column `count`
        is required to calculate the correct total width.

        This current implementation works best for DXF R2018, because the
        content is stored as a continuous text in a single MTEXT entity. For
        DXF versions prior to R2018 the content should be distributed across
        multiple MTEXT entities (one entity per column), which is not done by
        `ezdxf`, but the result is correct for advanced DXF viewers and CAD
        application, which do the MTEXT content distribution completely by
        itself.

        Because of the current limitations the use of this method is not
        recommend. This situation may improve in future releases, but the exact
        rendering of the content will also slow down the processing speed
        dramatically.

        (requires DXF R2000)

        Args:
            content: column content as a single string
            width: column width
            gutter_width: distance between columns
            height: max. column height
            count: expected column count
            dxfattribs: additional DXF attributes

        """
        dxfversion = self.dxfversion
        if dxfversion < DXF2000:
            raise DXFVersionError("MTEXT requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        if dxfversion < const.DXF2018:
            mtext = make_dynamic_auto_height_columns_r2000(
                content, width, gutter_width, height, count, dxfattribs
            )
        else:
            mtext = make_dynamic_auto_height_columns_r2018(
                content, width, gutter_width, height, count, dxfattribs
            )
        if self.doc:
            self.doc.entitydb.add(mtext)
        self.add_entity(mtext)
        return mtext

    def add_mtext_dynamic_manual_height_columns(
        self,
        content: str,
        width: float,
        gutter_width: float,
        heights: Sequence[float],
        dxfattribs=None,
    ) -> MText:
        """Add a multiline text entity with dynamic columns as
        :class:`~ezdxf.entities.MText` entity. The content is spread
        across the columns automatically by the CAD application.
        The `heights` sequence determine the height of the columns, except for
        the last column, which always takes the remaining content. The height
        value for the last column is required but can be 0, because the value
        is ignored. The count of `heights` also determines the count of columns,
        and :code:`max(heights)` defines the total height of the MTEXT entity,
        which may be wrong if the last column requires more space.

        This current implementation works best for DXF R2018, because the
        content is stored as a continuous text in a single MTEXT entity. For
        DXF versions prior to R2018 the content should be distributed across
        multiple MTEXT entities (one entity per column), which is not done by
        `ezdxf`, but the result is correct for advanced DXF viewers and CAD
        application, which do the MTEXT content distribution completely by
        itself.

        (requires DXF R2000)

        Args:
            content: column content as a single string
            width: column width
            gutter_width: distance between columns
            heights: column height for each column
            dxfattribs: additional DXF attributes

        """
        # The current implementation work well for R2018.
        #
        # For the prior DXF versions the current implementation puts the whole
        # content into the first (main) column.
        # This works for AutoCAD and BricsCAD, both collect the content from
        # the linked MTEXT columns and the main column and do their own MTEXT
        # rendering - DO trust the DXF content!
        # The drawing add-on will fail at this until a usable MTEXT renderer
        # is implemented. For now the drawing add-on renders the main MTEXT and
        # the linked MTEXT columns as standalone MTEXT entities, and all content
        # is stored in the main MTEXT entity.
        # For the same reason the R2018 MTEXT rendering is not correct.
        # In DXF R2018 the MTEXT entity is a single entity without linked
        # MTEXT columns and the content is a single string, which has to be
        # parsed, rendered and distributed across the columns by the
        # CAD application itself.

        dxfversion = self.dxfversion
        if dxfversion < DXF2000:
            raise DXFVersionError("MTEXT requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        if dxfversion < const.DXF2018:
            mtext = make_dynamic_manual_height_columns_r2000(
                content, width, gutter_width, heights, dxfattribs
            )
        else:
            mtext = make_dynamic_manual_height_columns_r2018(
                content, width, gutter_width, heights, dxfattribs
            )
        if self.doc:
            self.doc.entitydb.add(mtext)
        self.add_entity(mtext)
        return mtext

    def add_ray(self, start: UVec, unit_vector: UVec, dxfattribs=None) -> Ray:
        """
        Add a :class:`~ezdxf.entities.Ray` that begins at `start` point and
        continues to infinity (construction line). (requires DXF R2000)

        Args:
            start: location 3D point in :ref:`WCS`
            unit_vector: 3D vector (x, y, z)
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("RAY requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["start"] = Vec3(start)
        dxfattribs["unit_vector"] = Vec3(unit_vector).normalize()
        return self.new_entity("RAY", dxfattribs)  # type: ignore

    def add_xline(
        self, start: UVec, unit_vector: UVec, dxfattribs=None
    ) -> XLine:
        """Add an infinity :class:`~ezdxf.entities.XLine` (construction line).
        (requires DXF R2000)

        Args:
            start: location 3D point in :ref:`WCS`
            unit_vector: 3D vector (x, y, z)
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("XLINE requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["start"] = Vec3(start)
        dxfattribs["unit_vector"] = Vec3(unit_vector).normalize()
        return self.new_entity("XLINE", dxfattribs)  # type: ignore

    def add_spline(
        self,
        fit_points: Optional[Iterable[UVec]] = None,
        degree: int = 3,
        dxfattribs=None,
    ) -> Spline:
        """Add a B-spline (:class:`~ezdxf.entities.Spline` entity) defined by
        the given `fit_points` - the control points and knot values are created
        by the CAD application, therefore it is not predictable how the rendered
        spline will look like, because for every set of fit points exists an
        infinite set of B-splines.

        If `fit_points` is ``None``, an "empty" spline will be created, all data
        has to be set by the user.

        The SPLINE entity requires DXF R2000.

        AutoCAD creates a spline through fit points by a global curve
        interpolation and an unknown method to estimate the direction of the
        start- and end tangent.

        .. seealso::

            - :ref:`tut_spline`
            - :func:`ezdxf.math.fit_points_to_cad_cv`

        Args:
            fit_points: iterable of fit points as ``(x, y[, z])`` in :ref:`WCS`,
                creates an empty :class:`~ezdxf.entities.Spline` if ``None``
            degree: degree of B-spline, max. degree supported by AutoCAD is 11
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("SPLINE requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["degree"] = int(degree)
        spline: Spline = self.new_entity("SPLINE", dxfattribs)  # type: ignore
        if fit_points is not None:
            spline.fit_points = Vec3.generate(fit_points)
        return spline

    def add_spline_control_frame(
        self,
        fit_points: Iterable[UVec],
        degree: int = 3,
        method: str = "chord",
        dxfattribs=None,
    ) -> Spline:
        """Add a :class:`~ezdxf.entities.Spline` entity passing through the
        given `fit_points`, the control points are calculated by a global curve
        interpolation without start- and end tangent constrains.
        The new SPLINE entity is defined by control points and not by the fit
        points, therefore the SPLINE looks always the same, no matter which CAD
        application renders the SPLINE.

        - "uniform": creates a uniform t vector, from 0 to 1 evenly spaced, see
          `uniform`_ method
        - "distance", "chord": creates a t vector with values proportional to
          the fit point distances, see `chord length`_ method
        - "centripetal", "sqrt_chord": creates a t vector with values
          proportional to the fit point sqrt(distances), see `centripetal`_
          method
        - "arc": creates a t vector with values proportional to the arc length
          between fit points.

        Use function :meth:`add_cad_spline_control_frame` to create
        SPLINE entities from fit points similar to CAD application including
        start- and end tangent constraints.

        Args:
            fit_points: iterable of fit points as (x, y[, z]) in :ref:`WCS`
            degree: degree of B-spline, max. degree supported by AutoCAD is 11
            method: calculation method for parameter vector t
            dxfattribs: additional DXF attributes

        """
        bspline = global_bspline_interpolation(
            fit_points, degree=degree, method=method
        )
        return self.add_open_spline(
            control_points=bspline.control_points,
            degree=bspline.degree,
            knots=bspline.knots(),
            dxfattribs=dxfattribs,
        )

    def add_cad_spline_control_frame(
        self,
        fit_points: Iterable[UVec],
        tangents: Optional[Iterable[UVec]] = None,
        dxfattribs=None,
    ) -> Spline:
        """Add a :class:`~ezdxf.entities.Spline` entity passing through the
        given fit points.  This method creates the same control points as CAD 
        applications.

        Args:
            fit_points: iterable of fit points as (x, y[, z]) in :ref:`WCS`
            tangents: start- and end tangent, default is autodetect
            dxfattribs: additional DXF attributes

        """
        s = fit_points_to_cad_cv(fit_points, tangents=tangents)
        spline = self.add_spline(dxfattribs=dxfattribs)
        spline.apply_construction_tool(s)
        return spline

    def add_open_spline(
        self,
        control_points: Iterable[UVec],
        degree: int = 3,
        knots: Optional[Iterable[float]] = None,
        dxfattribs=None,
    ) -> Spline:
        """
        Add an open uniform :class:`~ezdxf.entities.Spline` defined by
        `control_points`. (requires DXF R2000)

        Open uniform B-splines start and end at your first and last control point.

        Args:
            control_points: iterable of 3D points in :ref:`WCS`
            degree: degree of B-spline, max. degree supported by AutoCAD is 11
            knots: knot values as iterable of floats
            dxfattribs: additional DXF attributes

        """
        spline = self.add_spline(dxfattribs=dxfattribs)
        spline.set_open_uniform(list(control_points), degree)
        if knots is not None:
            spline.knots = knots  # type: ignore
        return spline

    def add_rational_spline(
        self,
        control_points: Iterable[UVec],
        weights: Sequence[float],
        degree: int = 3,
        knots: Optional[Iterable[float]] = None,
        dxfattribs=None,
    ) -> Spline:
        """
        Add an open rational uniform :class:`~ezdxf.entities.Spline` defined by
        `control_points`. (requires DXF R2000)

        `weights` has to be an iterable of floats, which defines the influence
        of the associated control point to the shape of the B-spline, therefore
        for each control point is one weight value required.

        Open rational uniform B-splines start and end at the first and last
        control point.

        Args:
            control_points: iterable of 3D points in :ref:`WCS`
            weights: weight values as iterable of floats
            degree: degree of B-spline, max. degree supported by AutoCAD is 11
            knots: knot values as iterable of floats
            dxfattribs: additional DXF attributes

        """
        spline = self.add_spline(dxfattribs=dxfattribs)
        spline.set_open_rational(list(control_points), weights, degree)
        if knots is not None:
            spline.knots = knots  # type: ignore
        return spline

    def add_body(self, dxfattribs=None) -> Body:
        """Add a :class:`~ezdxf.entities.Body` entity.
        (requires DXF R2000 or later)

        The ACIS data has to be set as :term:`SAT` or :term:`SAB`.

        """
        return self._add_acis_entity("BODY", dxfattribs)

    def add_region(self, dxfattribs=None) -> Region:
        """Add a :class:`~ezdxf.entities.Region` entity.
        (requires DXF R2000 or later)

        The ACIS data has to be set as :term:`SAT` or :term:`SAB`.

        """
        return self._add_acis_entity("REGION", dxfattribs)  # type: ignore

    def add_3dsolid(self, dxfattribs=None) -> Solid3d:
        """Add a 3DSOLID entity (:class:`~ezdxf.entities.Solid3d`).
        (requires DXF R2000 or later)

        The ACIS data has to be set as :term:`SAT` or :term:`SAB`.

        """
        return self._add_acis_entity("3DSOLID", dxfattribs)  # type: ignore

    def add_surface(self, dxfattribs=None) -> Surface:
        """Add a :class:`~ezdxf.entities.Surface` entity.
        (requires DXF R2007 or later)

        The ACIS data has to be set as :term:`SAT` or :term:`SAB`.

        """
        if self.dxfversion < const.DXF2007:
            raise DXFVersionError("SURFACE requires DXF R2007 or later")
        return self._add_acis_entity("SURFACE", dxfattribs)  # type: ignore

    def add_extruded_surface(self, dxfattribs=None) -> ExtrudedSurface:
        """Add a :class:`~ezdxf.entities.ExtrudedSurface` entity.
        (requires DXF R2007 or later)

        The ACIS data has to be set as :term:`SAT` or :term:`SAB`.

        """
        if self.dxfversion < const.DXF2007:
            raise DXFVersionError("EXTRUDEDSURFACE requires DXF R2007 or later")
        return self._add_acis_entity("EXTRUDEDSURFACE", dxfattribs)  # type: ignore

    def add_lofted_surface(self, dxfattribs=None) -> LoftedSurface:
        """Add a :class:`~ezdxf.entities.LoftedSurface` entity.
        (requires DXF R2007 or later)

        The ACIS data has to be set as :term:`SAT` or :term:`SAB`.

        """
        if self.dxfversion < const.DXF2007:
            raise DXFVersionError("LOFTEDSURFACE requires DXF R2007 or later")
        return self._add_acis_entity("LOFTEDSURFACE", dxfattribs)  # type: ignore

    def add_revolved_surface(self, dxfattribs=None) -> RevolvedSurface:
        """
        Add a :class:`~ezdxf.entities.RevolvedSurface` entity.
        (requires DXF R2007 or later)

        The ACIS data has to be set as :term:`SAT` or :term:`SAB`.

        """
        if self.dxfversion < const.DXF2007:
            raise DXFVersionError("REVOLVEDSURFACE requires DXF R2007 or later")
        return self._add_acis_entity("REVOLVEDSURFACE", dxfattribs)  # type: ignore

    def add_swept_surface(self, dxfattribs=None) -> SweptSurface:
        """
        Add a :class:`~ezdxf.entities.SweptSurface` entity.
        (requires DXF R2007 or later)

        The ACIS data has to be set as :term:`SAT` or :term:`SAB`.

        """
        if self.dxfversion < const.DXF2007:
            raise DXFVersionError("SWEPTSURFACE requires DXF R2007 or later")
        return self._add_acis_entity("SWEPTSURFACE", dxfattribs)  # type: ignore

    def _add_acis_entity(self, name, dxfattribs) -> Body:
        if self.dxfversion < const.DXF2000:
            raise DXFVersionError(f"{name} requires DXF R2000 or later")
        dxfattribs = dict(dxfattribs or {})
        if self.dxfversion >= DXF2013:
            dxfattribs.setdefault("flags", 1)
            dxfattribs.setdefault("uid", guid())
        return self.new_entity(name, dxfattribs)  # type: ignore

    def add_hatch(self, color: int = 7, dxfattribs=None) -> Hatch:
        """Add a :class:`~ezdxf.entities.Hatch` entity. (requires DXF R2000)

        Args:
            color: fill color as :ref`ACI`, default is 7 (black/white).
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("HATCH requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["solid_fill"] = 1
        dxfattribs["color"] = int(color)
        dxfattribs["pattern_name"] = "SOLID"
        return self.new_entity("HATCH", dxfattribs)  # type: ignore

    def add_mpolygon(
        self,
        color: int = const.BYLAYER,
        fill_color: Optional[int] = None,
        dxfattribs=None,
    ) -> MPolygon:
        """Add a :class:`~ezdxf.entities.MPolygon` entity. (requires DXF R2000)

        The MPOLYGON entity is not a core DXF entity and is not supported by
        every CAD application or DXF library.

        DXF version R2004+ is required to use a fill color different from
        BYLAYER. For R2000 the fill color is always BYLAYER, set any ACI value
        to create a filled MPOLYGON entity.

        Args:
            color: boundary color as :ref:`ACI`, default is BYLAYER.
            fill_color: fill color as :ref:`ACI`, default is ``None``
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("MPOLYGON requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        if fill_color is None:
            dxfattribs["solid_fill"] = 0
        else:
            dxfattribs["solid_fill"] = 1
            dxfattribs["fill_color"] = fill_color

        dxfattribs["pattern_name"] = "SOLID"
        dxfattribs["pattern_type"] = const.HATCH_TYPE_PREDEFINED
        dxfattribs["color"] = int(color)
        return self.new_entity("MPOLYGON", dxfattribs)  # type: ignore

    def add_mesh(self, dxfattribs=None) -> Mesh:
        """
        Add a :class:`~ezdxf.entities.Mesh` entity. (requires DXF R2007)

        Args:
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("MESH requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        return self.new_entity("MESH", dxfattribs)  # type: ignore

    def add_image(
        self,
        image_def: ImageDef,
        insert: UVec,
        size_in_units: tuple[float, float],
        rotation: float = 0.0,
        dxfattribs=None,
    ) -> Image:
        """
        Add an :class:`~ezdxf.entities.Image` entity, requires a
        :class:`~ezdxf.entities.ImageDef` entity, see :ref:`tut_image`.
        (requires DXF R2000)

        Args:
            image_def: required image definition as :class:`~ezdxf.entities.ImageDef`
            insert: insertion point as 3D point in :ref:`WCS`
            size_in_units: size as (x, y) tuple in drawing units
            rotation: rotation angle around the extrusion axis, default is the
                z-axis, in degrees
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("IMAGE requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        x_pixels, y_pixels = image_def.dxf.image_size.vec2
        x_units, y_units = size_in_units
        x_units_per_pixel = x_units / x_pixels
        y_units_per_pixel = y_units / y_pixels
        x_angle_rad = math.radians(rotation)
        y_angle_rad = x_angle_rad + (math.pi / 2.0)

        dxfattribs["insert"] = Vec3(insert)
        dxfattribs["u_pixel"] = Vec3.from_angle(x_angle_rad, x_units_per_pixel)
        dxfattribs["v_pixel"] = Vec3.from_angle(y_angle_rad, y_units_per_pixel)
        dxfattribs["image_def"] = image_def  # is not a real DXF attrib
        return self.new_entity("IMAGE", dxfattribs)  # type: ignore

    def add_wipeout(self, vertices: Iterable[UVec], dxfattribs=None) -> Wipeout:
        """Add a :class:`ezdxf.entities.Wipeout` entity, the masking area is
        defined by WCS `vertices`.

        This method creates only a 2D entity in the xy-plane of the layout,
        the z-axis of the input vertices are ignored.

        """
        dxfattribs = dict(dxfattribs or {})
        wipeout: Wipeout = self.new_entity("WIPEOUT", dxfattribs=dxfattribs)  # type: ignore
        wipeout.set_masking_area(vertices)
        doc = self.doc
        if doc and ("ACAD_WIPEOUT_VARS" not in doc.rootdict):
            doc.set_wipeout_variables(frame=0)
        return wipeout

    def add_underlay(
        self,
        underlay_def: UnderlayDefinition,
        insert: UVec = (0, 0, 0),
        scale=(1, 1, 1),
        rotation: float = 0.0,
        dxfattribs=None,
    ) -> Underlay:
        """
        Add an :class:`~ezdxf.entities.Underlay` entity, requires a
        :class:`~ezdxf.entities.UnderlayDefinition` entity,
        see :ref:`tut_underlay`.  (requires DXF R2000)

        Args:
            underlay_def: required underlay definition as :class:`~ezdxf.entities.UnderlayDefinition`
            insert: insertion point as 3D point in :ref:`WCS`
            scale:  underlay scaling factor as (x, y, z) tuple or as single
                value for uniform scaling for x, y and z
            rotation: rotation angle around the extrusion axis, default is the
                z-axis, in degrees
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("UNDERLAY requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["insert"] = Vec3(insert)
        dxfattribs["underlay_def_handle"] = underlay_def.dxf.handle
        dxfattribs["rotation"] = float(rotation)

        underlay: Underlay = self.new_entity(  # type: ignore
            underlay_def.entity_name, dxfattribs
        )
        underlay.scaling = scale
        underlay.set_underlay_def(underlay_def)
        return underlay

    def _safe_dimstyle(self, name: str) -> str:
        if self.doc and self.doc.dimstyles.has_entry(name):
            return name
        logger.debug(
            f'Replacing undefined DIMSTYLE "{name}" by "Standard" DIMSTYLE.'
        )
        return "Standard"

    def add_linear_dim(
        self,
        base: UVec,
        p1: UVec,
        p2: UVec,
        location: Optional[UVec] = None,
        text: str = "<>",
        angle: float = 0,
        # 0=horizontal, 90=vertical, else=rotated
        text_rotation: Optional[float] = None,
        dimstyle: str = "EZDXF",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Add horizontal, vertical and rotated :class:`~ezdxf.entities.Dimension`
        line. If an :class:`~ezdxf.math.UCS` is used for dimension line rendering,
        all point definitions in UCS coordinates, translation into :ref:`WCS`
        and :ref:`OCS` is done by the rendering function. Extrusion vector is
        defined by UCS or (0, 0, 1) by default.
        See also: :ref:`tut_linear_dimension`

        This method returns a :class:`~ezdxf.entities.DimStyleOverride` object -
        to create the necessary dimension geometry, you have to call
        :meth:`~ezdxf.entities.DimStyleOverride.render` manually, this two-step
        process allows additional processing steps on the
        :class:`~ezdxf.entities.Dimension` entity between creation and rendering.

        .. note::

            `Ezdxf` does not consider all DIMSTYLE variables, so the
            rendering results are different from CAD applications.

        Args:
            base: location of dimension line, any point on the dimension line or
                its extension will do (in UCS)
            p1: measurement point 1 and start point of extension line 1 (in UCS)
            p2: measurement point 2 and start point of extension line 2 (in UCS)
            location: user defined location for the text midpoint (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text, everything
                else `text` is drawn as dimension text
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZDXF"
            angle: angle from ucs/wcs x-axis to dimension line in degrees
            text_rotation: rotation angle of the dimension text as absolute
                angle (x-axis=0, y-axis=90) in degrees
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        type_ = {"dimtype": const.DIM_LINEAR | const.DIM_BLOCK_EXCLUSIVE}
        dimline: Dimension = self.new_entity("DIMENSION", dxfattribs=type_)  # type: ignore
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["dimstyle"] = self._safe_dimstyle(dimstyle)
        dxfattribs["defpoint"] = Vec3(base)  # group code 10
        dxfattribs["text"] = str(text)
        dxfattribs["defpoint2"] = Vec3(p1)  # group code 13
        dxfattribs["defpoint3"] = Vec3(p2)  # group code 14
        dxfattribs["angle"] = float(angle)

        # text_rotation ALWAYS overrides implicit angles as absolute angle
        # (x-axis=0, y-axis=90)!
        if text_rotation is not None:
            dxfattribs["text_rotation"] = float(text_rotation)
        dimline.update_dxf_attribs(dxfattribs)

        style = DimStyleOverride(dimline, override=override)
        if location is not None:
            # special version, just for linear dimension
            style.set_location(location, leader=False, relative=False)
        return style

    def add_multi_point_linear_dim(
        self,
        base: UVec,
        points: Iterable[UVec],
        angle: float = 0,
        ucs: Optional[UCS] = None,
        avoid_double_rendering: bool = True,
        dimstyle: str = "EZDXF",
        override: Optional[dict] = None,
        dxfattribs=None,
        discard=False,
    ) -> None:
        """
        Add multiple linear dimensions for iterable `points`. If an
        :class:`~ezdxf.math.UCS` is used for dimension line rendering, all point
        definitions in UCS coordinates, translation into :ref:`WCS` and
        :ref:`OCS` is done by the rendering function. Extrusion vector is
        defined by UCS or (0, 0, 1) by default. See also:
        :ref:`tut_linear_dimension`

        This method sets many design decisions by itself, the necessary geometry
        will be generated automatically, no required nor possible
        :meth:`~ezdxf.entities.DimStyleOverride.render` call.
        This method is easy to use, but you get what you get.

        .. note::

            `Ezdxf` does not consider all DIMSTYLE variables, so the
            rendering results are different from CAD applications.

        Args:
            base: location of dimension line, any point on the dimension line
                or its extension will do (in UCS)
            points: iterable of measurement points (in UCS)
            angle: angle from ucs/wcs x-axis to dimension line in degrees
                (0 = horizontal, 90 = vertical)
            ucs: user defined coordinate system
            avoid_double_rendering: suppresses the first extension line and the
                first arrow if possible for continued dimension entities
            dimstyle: dimension style name (DimStyle table entry),
                default is "EZDXF"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity
            discard: discard rendering result for friendly CAD applications like
                BricsCAD to get a native and likely better rendering result.
                (does not work with AutoCAD)

        """
        dxfattribs = dict(dxfattribs or {})
        multi_point_linear_dimension(
            cast("GenericLayoutType", self),
            base=base,
            points=points,
            angle=angle,
            ucs=ucs,
            avoid_double_rendering=avoid_double_rendering,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
            discard=discard,
        )

    def add_aligned_dim(
        self,
        p1: UVec,
        p2: UVec,
        distance: float,
        text: str = "<>",
        dimstyle: str = "EZDXF",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Add linear dimension aligned with measurement points `p1` and `p2`. If
        an :class:`~ezdxf.math.UCS` is used for dimension line rendering, all
        point definitions in UCS coordinates, translation into :ref:`WCS`
        and :ref:`OCS` is done by the rendering function. Extrusion vector
        is defined by UCS or (0, 0, 1) by default.
        See also: :ref:`tut_linear_dimension`

        This method returns a :class:`~ezdxf.entities.DimStyleOverride` object,
        to create the necessary dimension geometry, you have to call
        :meth:`DimStyleOverride.render` manually, this two-step process allows
        additional processing steps on the  :class:`~ezdxf.entities.Dimension`
        entity between creation and rendering.

        .. note::

            `Ezdxf` does not consider all DIMSTYLE variables, so the
            rendering results are different from CAD applications.

        Args:
            p1: measurement point 1 and start point of extension line 1 (in UCS)
            p2: measurement point 2 and start point of extension line 2 (in UCS)
            distance: distance of dimension line from measurement points
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZDXF"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs:  additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        _p1 = Vec3(p1)
        _p2 = Vec3(p2)
        direction = _p2 - _p1
        angle = direction.angle_deg
        base = direction.orthogonal().normalize(distance) + _p1
        return self.add_linear_dim(
            base=base,
            p1=_p1,
            p2=_p2,
            dimstyle=dimstyle,
            text=text,
            angle=angle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_angular_dim_2l(
        self,
        base: UVec,
        line1: tuple[UVec, UVec],
        line2: tuple[UVec, UVec],
        *,
        location: Optional[UVec] = None,
        text: str = "<>",
        text_rotation: Optional[float] = None,
        dimstyle: str = "EZ_CURVED",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Add angular :class:`~ezdxf.entities.Dimension` from two lines. The
        measurement is always done from `line1` to `line2` in counter-clockwise
        orientation. This does not always match the result in CAD applications!

        If an :class:`~ezdxf.math.UCS` is used for angular dimension rendering,
        all point definitions in UCS coordinates, translation into :ref:`WCS`
        and :ref:`OCS` is done by the rendering function. Extrusion vector is
        defined by UCS or (0, 0, 1) by default.

        This method returns a :class:`~ezdxf.entities.DimStyleOverride` object -
        to create the necessary dimension geometry, you have to call
        :meth:`~ezdxf.entities.DimStyleOverride.render` manually, this two-step
        process allows additional processing steps on the
        :class:`~ezdxf.entities.Dimension` entity between creation and rendering.

        .. note::

            `Ezdxf` does not consider all DIMSTYLE variables, so the
            rendering results are different from CAD applications.

        Args:
            base: location of dimension line, any point on the dimension line or
                its extension is valid (in UCS)
            line1: specifies start leg of the angle (start point, end point) and
                determines extension line 1 (in UCS)
            line2: specifies end leg of the angle (start point, end point) and
                determines extension line 2 (in UCS)
            location: user defined location for the text midpoint (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            text_rotation: rotation angle of the dimension text as absolute
                angle (x-axis=0, y-axis=90) in degrees
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_CURVED"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs:  additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        type_ = {"dimtype": const.DIM_ANGULAR | const.DIM_BLOCK_EXCLUSIVE}
        dimline: Dimension = self.new_entity("DIMENSION", dxfattribs=type_)  # type: ignore

        dxfattribs = dict(dxfattribs or {})
        dxfattribs["dimstyle"] = self._safe_dimstyle(dimstyle)
        dxfattribs["text"] = str(text)

        dxfattribs["defpoint2"] = Vec3(line1[0])  # group code 13
        dxfattribs["defpoint3"] = Vec3(line1[1])  # group code 14
        dxfattribs["defpoint4"] = Vec3(line2[0])  # group code 15
        dxfattribs["defpoint"] = Vec3(line2[1])  # group code 10
        dxfattribs["defpoint5"] = Vec3(base)  # group code 16

        # text_rotation ALWAYS overrides implicit angles as absolute angle (x-axis=0, y-axis=90)!
        if text_rotation is not None:
            dxfattribs["text_rotation"] = float(text_rotation)

        dimline.update_dxf_attribs(dxfattribs)
        style = DimStyleOverride(dimline, override=override)
        if location is not None:
            style.user_location_override(location)
        return style

    def add_angular_dim_3p(
        self,
        base: UVec,
        center: UVec,
        p1: UVec,
        p2: UVec,
        *,
        location: Optional[UVec] = None,
        text: str = "<>",
        text_rotation: Optional[float] = None,
        dimstyle: str = "EZ_CURVED",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Add angular :class:`~ezdxf.entities.Dimension` from three points
        (center, p1, p2). The measurement is always done from `p1` to `p2` in
        counter-clockwise orientation. This does not always match the result in
        CAD applications!

        If an :class:`~ezdxf.math.UCS` is used for angular dimension rendering,
        all point definitions in UCS coordinates, translation into :ref:`WCS`
        and :ref:`OCS` is done by the rendering function. Extrusion vector is
        defined by UCS or (0, 0, 1) by default.

        This method returns a :class:`~ezdxf.entities.DimStyleOverride` object -
        to create the necessary dimension geometry, you have to call
        :meth:`~ezdxf.entities.DimStyleOverride.render` manually, this two-step
        process allows additional processing steps on the
        :class:`~ezdxf.entities.Dimension` entity between creation and rendering.

        .. note::

            `Ezdxf` does not consider all DIMSTYLE variables, so the
            rendering results are different from CAD applications.

        Args:
            base: location of dimension line, any point on the dimension line
                or its extension is valid (in UCS)
            center: specifies the vertex of the angle
            p1: specifies start leg of the angle (center -> p1) and end-point
                of extension line 1 (in UCS)
            p2: specifies end leg of the  angle (center -> p2) and end-point
                of extension line 2 (in UCS)
            location: user defined location for the text midpoint (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            text_rotation: rotation angle of the dimension text as absolute
                angle (x-axis=0, y-axis=90) in degrees
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_CURVED"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        type_ = {"dimtype": const.DIM_ANGULAR_3P | const.DIM_BLOCK_EXCLUSIVE}
        dimline = cast(
            "Dimension", self.new_entity("DIMENSION", dxfattribs=type_)
        )
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["dimstyle"] = self._safe_dimstyle(dimstyle)
        dxfattribs["text"] = str(text)
        dxfattribs["defpoint"] = Vec3(base)
        dxfattribs["defpoint2"] = Vec3(p1)
        dxfattribs["defpoint3"] = Vec3(p2)
        dxfattribs["defpoint4"] = Vec3(center)

        # text_rotation ALWAYS overrides implicit angles as absolute angle
        # (x-axis=0, y-axis=90)!
        if text_rotation is not None:
            dxfattribs["text_rotation"] = float(text_rotation)

        dimline.update_dxf_attribs(dxfattribs)
        style = DimStyleOverride(dimline, override=override)
        if location is not None:
            style.user_location_override(location)
        return style

    def add_angular_dim_cra(
        self,
        center: UVec,
        radius: float,
        start_angle: float,
        end_angle: float,
        distance: float,
        *,
        location: Optional[UVec] = None,
        text: str = "<>",
        text_rotation: Optional[float] = None,
        dimstyle: str = "EZ_CURVED",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Shortcut method to create an angular dimension by (c)enter point,
        (r)adius and start- and end (a)ngles, the measurement text is placed at
        the default location defined by the associated `dimstyle`.
        The measurement is always done from `start_angle` to `end_angle` in
        counter-clockwise orientation. This does not always match the result in
        CAD applications!
        For further information see the more generic factory method
        :func:`add_angular_dim_3p`.

        Args:
            center: center point of the angle (in UCS)
            radius: the distance from `center` to the start of the extension
                lines in drawing units
            start_angle: start angle in degrees (in UCS)
            end_angle: end angle in degrees (in UCS)
            distance: distance from start of the extension lines to the
                dimension line in drawing units
            location: user defined location for the text midpoint (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            text_rotation: rotation angle of the dimension text as absolute
                angle (x-axis=0, y-axis=90) in degrees
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_CURVED"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        sa = float(start_angle)
        ea = float(end_angle)
        ext_line_start = float(radius)
        dim_line_offset = float(distance)
        center_ = Vec3(center)

        center_angle = sa + arc_angle_span_deg(sa, ea) / 2.0
        # ca = (sa + ea) / 2 is not correct: e.g. 30, -30 is 0 but should be 180

        base = center_ + Vec3.from_deg_angle(center_angle) * (
            ext_line_start + dim_line_offset
        )
        p1 = center_ + Vec3.from_deg_angle(sa) * ext_line_start
        p2 = center_ + Vec3.from_deg_angle(ea) * ext_line_start
        return self.add_angular_dim_3p(
            base=base,
            center=center_,
            p1=p1,
            p2=p2,
            location=location,
            text=text,
            text_rotation=text_rotation,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_angular_dim_arc(
        self,
        arc: ConstructionArc,
        distance: float,
        *,
        location: Optional[UVec] = None,
        text: str = "<>",
        text_rotation: Optional[float] = None,
        dimstyle: str = "EZ_CURVED",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Shortcut method to create an angular dimension from a
        :class:`~ezdxf.math.ConstructionArc`. This construction tool can
        be created from ARC entities and the tool itself provides various
        construction class methods.
        The measurement text is placed at the default location defined by the
        associated `dimstyle`.
        The measurement is always done from `start_angle` to `end_angle` of the
        arc in counter-clockwise orientation.
        This does not always match the result in CAD applications!
        For further information see the more generic factory method
        :func:`add_angular_dim_3p`.

        Args:
            arc: :class:`~ezdxf.math.ConstructionArc`
            distance: distance from start of the extension lines to the
                dimension line in drawing units
            location: user defined location for the text midpoint (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            text_rotation: rotation angle of the dimension text as absolute
                angle (x-axis=0, y-axis=90) in degrees
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_CURVED"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs:  additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        return self.add_angular_dim_cra(
            center=arc.center,
            radius=arc.radius,
            start_angle=arc.start_angle,
            end_angle=arc.end_angle,
            distance=distance,
            location=location,
            text=text,
            text_rotation=text_rotation,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_arc_dim_3p(
        self,
        base: UVec,
        center: UVec,
        p1: UVec,
        p2: UVec,
        *,
        location: Optional[UVec] = None,
        text: str = "<>",
        text_rotation: Optional[float] = None,
        dimstyle: str = "EZ_CURVED",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Add :class:`~ezdxf.entities.ArcDimension` from three points
        (center, p1, p2). Point `p1` defines the radius and the start-angle of
        the arc, point `p2` only defines the end-angle of the arc.

        If an :class:`~ezdxf.math.UCS` is used for arc dimension rendering,
        all point definitions in UCS coordinates, translation into :ref:`WCS`
        and :ref:`OCS` is done by the rendering function. Extrusion vector is
        defined by UCS or (0, 0, 1) by default.

        This method returns a :class:`~ezdxf.entities.DimStyleOverride` object -
        to create the necessary dimension geometry, you have to call
        :meth:`~ezdxf.entities.DimStyleOverride.render` manually, this two-step
        process allows additional processing steps on the
        :class:`~ezdxf.entities.ArcDimension` entity between creation and
        rendering.

        .. note::

            `Ezdxf` does not render the arc dimension like CAD applications and
            does not consider all DIMSTYLE variables, so the rendering results
            are **very** different from CAD applications.

        Args:
            base: location of dimension line, any point on the dimension line
                or its extension is valid (in UCS)
            center: specifies the vertex of the angle
            p1: specifies the radius (center -> p1) and the star angle of the
                arc, this is also the start point for the 1st extension line (in UCS)
            p2: specifies the end angle of the arc. The start 2nd extension line
                is defined by this angle and the radius defined by p1 (in UCS)
            location: user defined location for the text midpoint (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            text_rotation: rotation angle of the dimension text as absolute
                angle (x-axis=0, y-axis=90) in degrees
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_CURVED"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        # always set dimtype to 8 for DXF R2013+, the DXF export handles the
        # version dependent dimtype
        type_ = {"dimtype": const.DIM_ARC | const.DIM_BLOCK_EXCLUSIVE}
        dimline: ArcDimension = self.new_entity(  # type: ignore
            "ARC_DIMENSION", dxfattribs=type_
        )
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["dimstyle"] = self._safe_dimstyle(dimstyle)
        dxfattribs["text"] = str(text)
        dxfattribs["defpoint"] = Vec3(base)
        dxfattribs["defpoint2"] = Vec3(p1)
        dxfattribs["defpoint3"] = Vec3(p2)
        dxfattribs["defpoint4"] = Vec3(center)
        dxfattribs["start_angle"] = 0.0  # unknown meaning
        dxfattribs["end_angle"] = 0.0  # unknown meaning
        dxfattribs["is_partial"] = 0  # unknown meaning
        dxfattribs["has_leader"] = 0  # ignored by ezdxf
        dxfattribs["leader_point1"] = NULLVEC  # ignored by ezdxf
        dxfattribs["leader_point2"] = NULLVEC  # ignored by ezdxf

        # text_rotation ALWAYS overrides implicit angles as absolute angle
        # (x-axis=0, y-axis=90)!
        if text_rotation is not None:
            dxfattribs["text_rotation"] = float(text_rotation)

        dimline.update_dxf_attribs(dxfattribs)
        style = DimStyleOverride(dimline, override=override)
        if location is not None:
            style.user_location_override(location)
        return style

    def add_arc_dim_cra(
        self,
        center: UVec,
        radius: float,
        start_angle: float,
        end_angle: float,
        distance: float,
        *,
        location: Optional[UVec] = None,
        text: str = "<>",
        text_rotation: Optional[float] = None,
        dimstyle: str = "EZ_CURVED",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Shortcut method to create an arc dimension by (c)enter point,
        (r)adius and start- and end (a)ngles, the measurement text is placed at
        the default location defined by the associated `dimstyle`.

        .. note::

            `Ezdxf` does not render the arc dimension like CAD applications and
            does not consider all DIMSTYLE variables, so the rendering results
            are **very** different from CAD applications.

        Args:
            center: center point of the angle (in UCS)
            radius: the distance from `center` to the start of the extension
                lines in drawing units
            start_angle: start-angle in degrees (in UCS)
            end_angle: end-angle in degrees (in UCS)
            distance: distance from start of the extension lines to the
                dimension line in drawing units
            location: user defined location for text midpoint (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            text_rotation: rotation angle of the dimension text as absolute
                angle (x-axis=0, y-axis=90) in degrees
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_CURVED"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        sa = float(start_angle)
        ea = float(end_angle)
        ext_line_start = float(radius)
        dim_line_offset = float(distance)
        center_ = Vec3(center)

        center_angle = sa + arc_angle_span_deg(sa, ea) / 2.0
        # ca = (sa + ea) / 2 is not correct: e.g. 30, -30 is 0 but should be 180

        base = center_ + Vec3.from_deg_angle(center_angle) * (
            ext_line_start + dim_line_offset
        )
        p1 = center_ + Vec3.from_deg_angle(sa) * ext_line_start
        p2 = center_ + Vec3.from_deg_angle(ea) * ext_line_start
        return self.add_arc_dim_3p(
            base=base,
            center=center_,
            p1=p1,
            p2=p2,
            location=location,
            text=text,
            text_rotation=text_rotation,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_arc_dim_arc(
        self,
        arc: ConstructionArc,
        distance: float,
        *,
        location: Optional[UVec] = None,
        text: str = "<>",
        text_rotation: Optional[float] = None,
        dimstyle: str = "EZ_CURVED",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Shortcut method to create an arc dimension from a
        :class:`~ezdxf.math.ConstructionArc`. This construction tool can
        be created from ARC entities and the tool itself provides various
        construction class methods.
        The measurement text is placed at the default location defined by the
        associated `dimstyle`.

        .. note::

            `Ezdxf` does not render the arc dimension like CAD applications and
            does not consider all DIMSTYLE variables, so the rendering results
            are **very** different from CAD applications.

        Args:
            arc: :class:`~ezdxf.math.ConstructionArc`
            distance: distance from start of the extension lines to the
                dimension line in drawing units
            location: user defined location for the text midpoint (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            text_rotation: rotation angle of the dimension text as absolute
                angle (x-axis=0, y-axis=90) in degrees
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_CURVED"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        return self.add_arc_dim_cra(
            center=arc.center,
            radius=arc.radius,
            start_angle=arc.start_angle,
            end_angle=arc.end_angle,
            distance=distance,
            location=location,
            text=text,
            text_rotation=text_rotation,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_diameter_dim(
        self,
        center: UVec,
        mpoint: Optional[UVec] = None,
        radius: Optional[float] = None,
        angle: Optional[float] = None,
        *,
        location: Optional[UVec] = None,
        text: str = "<>",
        dimstyle: str = "EZ_RADIUS",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Add a diameter :class:`~ezdxf.entities.Dimension` line. The diameter
        dimension line requires a `center` point and a point `mpoint` on the
        circle or as an alternative a `radius` and a dimension line `angle` in
        degrees.

        If an :class:`~ezdxf.math.UCS` is used for dimension line rendering,
        all point definitions in UCS coordinates, translation into :ref:`WCS`
        and :ref:`OCS` is done by the rendering function. Extrusion vector is
        defined by UCS or (0, 0, 1) by default.

        This method returns a :class:`~ezdxf.entities.DimStyleOverride` object -
        to create the necessary dimension geometry, you have to call
        :meth:`~ezdxf.entities.DimStyleOverride.render` manually, this two-step
        process allows additional processing steps on the :class:`~ezdxf.entities.Dimension`
        entity between creation and rendering.

        .. note::

            `Ezdxf` does not consider all DIMSTYLE variables, so the
            rendering results are different from CAD applications.

        Args:
            center: specifies the center of the circle (in UCS)
            mpoint: specifies the measurement point on the circle (in UCS)
            radius: specify radius, requires argument `angle`, overrides `p1` argument
            angle: specify angle of dimension line in degrees, requires argument
                `radius`, overrides `p1` argument
            location: user defined location for the text midpoint (in UCS)
            text: ``None`` or ``"<>"`` the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_RADIUS"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        type_ = {"dimtype": const.DIM_DIAMETER | const.DIM_BLOCK_EXCLUSIVE}
        dimline = cast(
            "Dimension", self.new_entity("DIMENSION", dxfattribs=type_)
        )
        center = Vec3(center)
        if location is not None:
            if radius is None:
                raise ValueError("Argument radius is required.")
            location = Vec3(location)

            # (center - location) just works as expected, but in my
            # understanding it should be: (location - center)
            radius_vec = (center - location).normalize(length=radius)
        else:  # defined by mpoint = measurement point on circle
            if mpoint is None:  # defined by radius and angle
                if angle is None:
                    raise ValueError("Argument angle or mpoint required.")
                if radius is None:
                    raise ValueError("Argument radius or mpoint required.")
                radius_vec = Vec3.from_deg_angle(angle, radius)
            else:
                radius_vec = Vec3(mpoint) - center

        p1 = center + radius_vec
        p2 = center - radius_vec
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["dimstyle"] = self._safe_dimstyle(dimstyle)
        dxfattribs["defpoint"] = Vec3(p1)  # group code 10
        dxfattribs["defpoint4"] = Vec3(p2)  # group code 15
        dxfattribs["text"] = str(text)

        dimline.update_dxf_attribs(dxfattribs)

        style = DimStyleOverride(dimline, override=override)
        if location is not None:
            style.user_location_override(location)
        return style

    def add_diameter_dim_2p(
        self,
        p1: UVec,
        p2: UVec,
        text: str = "<>",
        dimstyle: str = "EZ_RADIUS",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Shortcut method to create a diameter dimension by two points on the
        circle and the measurement text at the default location defined by the
        associated `dimstyle`, for further information see general method
        :func:`add_diameter_dim`. Center point of the virtual circle is the
        midpoint between `p1` and `p2`.

        - dimstyle "EZ_RADIUS": places the dimension text outside
        - dimstyle "EZ_RADIUS_INSIDE": places the dimension text inside

        Args:
            p1: first point of the circle (in UCS)
            p2: second point on the opposite side of the center point of the
                circle (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_RADIUS"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        mpoint = Vec3(p1)
        center = mpoint.lerp(p2)
        return self.add_diameter_dim(
            center,
            mpoint,
            text=text,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_radius_dim(
        self,
        center: UVec,
        mpoint: Optional[UVec] = None,
        radius: Optional[float] = None,
        angle: Optional[float] = None,
        *,
        location: Optional[UVec] = None,
        text: str = "<>",
        dimstyle: str = "EZ_RADIUS",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Add a radius :class:`~ezdxf.entities.Dimension` line. The radius
        dimension line requires a `center` point and a point `mpoint` on the
        circle or as an alternative a `radius` and a dimension line `angle` in
        degrees. See also: :ref:`tut_radius_dimension`

        If a :class:`~ezdxf.math.UCS` is used for dimension line rendering,
        all point definitions in UCS coordinates, translation into :ref:`WCS`
        and :ref:`OCS` is done by the rendering function. Extrusion vector is
        defined by UCS or (0, 0, 1) by default.

        This method returns a :class:`~ezdxf.entities.DimStyleOverride` object -
        to create the necessary dimension geometry, you have to call
        :meth:`~ezdxf.entities.DimStyleOverride.render` manually, this two-step
        process allows additional processing steps on the
        :class:`~ezdxf.entities.Dimension` entity between creation and rendering.

        Following render types are supported:

        - Default text location outside: text aligned with dimension line;
          dimension style: "EZ_RADIUS"
        - Default text location outside horizontal: "EZ_RADIUS" + dimtoh=1
        - Default text location inside: text aligned with dimension line;
          dimension style: "EZ_RADIUS_INSIDE"
        - Default text location inside horizontal: "EZ_RADIUS_INSIDE" + dimtih=1
        - User defined text location: argument `location` != ``None``, text
          aligned with dimension line; dimension style: "EZ_RADIUS"
        - User defined text location horizontal: argument `location` != ``None``,
          "EZ_RADIUS" + dimtoh=1 for text outside horizontal, "EZ_RADIUS"
          + dimtih=1 for text inside horizontal

        Placing the dimension text at a user defined `location`, overrides the
        `mpoint` and the `angle` argument, but requires a given `radius`
        argument. The `location` argument does not define the exact text
        location, instead it defines the dimension line starting at `center`
        and the measurement text midpoint projected on this dimension line
        going through `location`, if text is aligned to the dimension line.
        If text is horizontal, `location` is the kink point of the dimension
        line from radial to horizontal direction.

        .. note::

            `Ezdxf` does not consider all DIMSTYLE variables, so the
            rendering results are different from CAD applications.

        Args:
            center: center point of the circle (in UCS)
            mpoint: measurement point on the circle, overrides `angle` and
                `radius` (in UCS)
            radius: radius in drawing units, requires argument `angle`
            angle: specify angle of dimension line in degrees, requires
                argument `radius`
            location: user defined dimension text location, overrides `mpoint`
                and `angle`, but requires `radius` (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_RADIUS"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        type_ = {"dimtype": const.DIM_RADIUS | const.DIM_BLOCK_EXCLUSIVE}
        dimline = cast(
            "Dimension", self.new_entity("DIMENSION", dxfattribs=type_)
        )
        center = Vec3(center)
        if location is not None:
            if radius is None:
                raise ValueError("Argument radius is required.")
            location = Vec3(location)
            radius_vec = (location - center).normalize(length=radius)
            mpoint = center + radius_vec
        else:  # defined by mpoint = measurement point on circle
            if mpoint is None:  # defined by radius and angle
                if angle is None:
                    raise ValueError("Argument angle or mpoint required.")
                if radius is None:
                    raise ValueError("Argument radius or mpoint required.")
                mpoint = center + Vec3.from_deg_angle(angle, radius)

        dxfattribs = dict(dxfattribs or {})
        dxfattribs["dimstyle"] = self._safe_dimstyle(dimstyle)
        dxfattribs["defpoint4"] = Vec3(mpoint)  # group code 15
        dxfattribs["defpoint"] = Vec3(center)  # group code 10
        dxfattribs["text"] = str(text)

        dimline.update_dxf_attribs(dxfattribs)

        style = DimStyleOverride(dimline, override=override)
        if location is not None:
            style.user_location_override(location)

        return style

    def add_radius_dim_2p(
        self,
        center: UVec,
        mpoint: UVec,
        *,
        text: str = "<>",
        dimstyle: str = "EZ_RADIUS",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Shortcut method to create a radius dimension by center point,
        measurement point on the circle and the measurement text at the default
        location defined by the associated `dimstyle`, for further information
        see general method :func:`add_radius_dim`.

        - dimstyle "EZ_RADIUS": places the dimension text outside
        - dimstyle "EZ_RADIUS_INSIDE": places the dimension text inside

        Args:
            center: center point of the circle (in UCS)
            mpoint: measurement point on the circle (in UCS)
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_RADIUS"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        return self.add_radius_dim(
            center,
            mpoint,
            text=text,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_radius_dim_cra(
        self,
        center: UVec,
        radius: float,
        angle: float,
        *,
        text: str = "<>",
        dimstyle: str = "EZ_RADIUS",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Shortcut method to create a radius dimension by (c)enter point,
        (r)adius and (a)ngle, the measurement text is placed at the default
        location defined by the associated `dimstyle`, for further information
        see general method :func:`add_radius_dim`.

        - dimstyle "EZ_RADIUS": places the dimension text outside
        - dimstyle "EZ_RADIUS_INSIDE": places the dimension text inside

        Args:
            center: center point of the circle (in UCS)
            radius: radius in drawing units
            angle: angle of dimension line in degrees
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZ_RADIUS"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        return self.add_radius_dim(
            center,
            radius=radius,
            angle=angle,
            text=text,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_ordinate_dim(
        self,
        feature_location: UVec,
        offset: UVec,
        dtype: int,
        *,
        origin: UVec = NULLVEC,
        rotation: float = 0.0,
        text: str = "<>",
        dimstyle: str = "EZDXF",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """
        Add an ordinate type :class:`~ezdxf.entities.Dimension` line. The
        feature location is defined in the global coordinate system, which is
        set as render UCS, which is the :ref:`WCS` by default.

        If an :class:`~ezdxf.math.UCS` is used for dimension line rendering,
        all point definitions in UCS coordinates, translation into :ref:`WCS`
        and :ref:`OCS` is done by the rendering function. Extrusion vector is
        defined by UCS or (0, 0, 1) by default.

        This method returns a :class:`~ezdxf.entities.DimStyleOverride` object -
        to create the necessary dimension geometry, you have to call
        :meth:`~ezdxf.entities.DimStyleOverride.render` manually, this two-step
        process allows additional processing steps on the
        :class:`~ezdxf.entities.Dimension` entity between creation and rendering.

        .. note::

            `Ezdxf` does not consider all DIMSTYLE variables, so the
            rendering results are different from CAD applications.

        Args:
            feature_location: feature location in the global coordinate system (UCS)
            offset: offset vector of leader end point from the feature location
                in the local coordinate system
            dtype: 1 = x-type, 0 = y-type
            origin: specifies the origin (0, 0) of the local coordinate
                system in UCS
            rotation: rotation angle of the local coordinate system in degrees
            text: ``None`` or "<>" the measurement is drawn as text,
                " " (a single space) suppresses the dimension text,
                everything else `text` is drawn as dimension text
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZDXF"
            override: :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes for the DIMENSION entity

        Returns: :class:`~ezdxf.entities.DimStyleOverride`

        """
        dtype = int(dtype)
        if dtype not in (0, 1):
            raise DXFValueError("invalid dtype (0, 1)")

        type_ = {
            "dimtype": const.DIM_ORDINATE
            | const.DIM_BLOCK_EXCLUSIVE
            | (const.DIM_ORDINATE_TYPE * dtype)
        }

        dimline = cast(
            "Dimension", self.new_entity("DIMENSION", dxfattribs=type_)
        )
        origin_ = Vec3(origin)
        feature_location_ = Vec3(feature_location)
        end_point_ = feature_location_ + Vec3(offset)
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["dimstyle"] = self._safe_dimstyle(dimstyle)
        dxfattribs["defpoint"] = origin_  # group code 10
        rotation = float(rotation)
        if rotation:
            # Horizontal direction in clockwise orientation, see DXF reference
            # for group code 51:
            dxfattribs["horizontal_direction"] = -rotation

        relative_feature_location = feature_location_ - origin_
        dxfattribs[
            "defpoint2"
        ] = origin_ + relative_feature_location.rotate_deg(rotation)
        dxfattribs["defpoint3"] = end_point_.rotate_deg(rotation)
        dxfattribs["text"] = str(text)
        dimline.update_dxf_attribs(dxfattribs)

        style = DimStyleOverride(dimline, override=override)
        return style

    def add_ordinate_x_dim(
        self,
        feature_location: UVec,
        offset: UVec,
        *,
        origin: UVec = NULLVEC,
        rotation: float = 0.0,
        text: str = "<>",
        dimstyle: str = "EZDXF",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """Shortcut to add an x-type feature ordinate DIMENSION, for more
        information see :meth:`add_ordinate_dim`.

        """
        return self.add_ordinate_dim(
            feature_location=feature_location,
            offset=offset,
            dtype=1,
            origin=origin,
            rotation=rotation,
            text=text,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_ordinate_y_dim(
        self,
        feature_location: UVec,
        offset: UVec,
        *,
        origin: UVec = NULLVEC,
        rotation: float = 0.0,
        text: str = "<>",
        dimstyle: str = "EZDXF",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> DimStyleOverride:
        """Shortcut to add a y-type feature ordinate DIMENSION, for more
        information see :meth:`add_ordinate_dim`.

        """
        return self.add_ordinate_dim(
            feature_location=feature_location,
            offset=offset,
            dtype=0,
            origin=origin,
            rotation=rotation,
            text=text,
            dimstyle=dimstyle,
            override=override,
            dxfattribs=dxfattribs,
        )

    def add_arrow(
        self,
        name: str,
        insert: UVec,
        size: float = 1.0,
        rotation: float = 0,
        dxfattribs=None,
    ) -> Vec3:
        return ARROWS.render_arrow(
            self,  # type: ignore
            name=name,
            insert=insert,
            size=size,
            rotation=rotation,
            dxfattribs=dxfattribs,
        ).vec3

    def add_arrow_blockref(
        self,
        name: str,
        insert: UVec,
        size: float = 1.0,
        rotation: float = 0,
        dxfattribs=None,
    ) -> Vec3:
        return ARROWS.insert_arrow(
            self,  # type: ignore
            name=name,
            insert=insert,
            size=size,
            rotation=rotation,
            dxfattribs=dxfattribs,
        ).vec3

    def add_leader(
        self,
        vertices: Iterable[UVec],
        dimstyle: str = "EZDXF",
        override: Optional[dict] = None,
        dxfattribs=None,
    ) -> Leader:
        """
        The :class:`~ezdxf.entities.Leader` entity represents an arrow, made up
        of one or more vertices (or spline fit points) and an arrowhead.
        The label or other content to which the :class:`~ezdxf.entities.Leader`
        is attached is stored as a separate entity, and is not part of the
        :class:`~ezdxf.entities.Leader` itself. (requires DXF R2000)

        :class:`~ezdxf.entities.Leader` shares its styling infrastructure with
        :class:`~ezdxf.entities.Dimension`.

        By default a :class:`~ezdxf.entities.Leader` without any annotation is
        created. For creating more fancy leaders and annotations see
        documentation provided by Autodesk or `Demystifying DXF: LEADER and MULTILEADER
        implementation notes <https://atlight.github.io/formats/dxf-leader.html>`_  .


        Args:
            vertices: leader vertices (in :ref:`WCS`)
            dimstyle: dimension style name (:class:`~ezdxf.entities.DimStyle`
                table entry), default is "EZDXF"
            override: override :class:`~ezdxf.entities.DimStyleOverride` attributes
            dxfattribs: additional DXF attributes

        """

        def filter_unsupported_dimstyle_attributes(attribs: dict) -> dict:
            return {
                k: v
                for k, v in attribs.items()
                if k not in LEADER_UNSUPPORTED_DIMSTYLE_ATTRIBS
            }

        if self.dxfversion < DXF2000:
            raise DXFVersionError("LEADER requires DXF R2000")

        dxfattribs = dict(dxfattribs or {})
        dxfattribs["dimstyle"] = self._safe_dimstyle(dimstyle)
        dxfattribs.setdefault("annotation_type", 3)
        leader = cast("Leader", self.new_entity("LEADER", dxfattribs))
        leader.set_vertices(vertices)
        if override:
            override = filter_unsupported_dimstyle_attributes(override)
            if "dimldrblk" in override:
                self.doc.acquire_arrow(override["dimldrblk"])
            # Class Leader() supports the required OverrideMixin() interface
            DimStyleOverride(
                cast("Dimension", leader), override=override
            ).commit()
        return leader

    def add_multileader_mtext(
        self,
        style: str = "Standard",
        dxfattribs=None,
    ) -> MultiLeaderMTextBuilder:
        """Add a :class:`~ezdxf.entities.MultiLeader` entity but returns
        a :class:`~ezdxf.render.MultiLeaderMTextBuilder`.

        """
        from ezdxf.render.mleader import MultiLeaderMTextBuilder

        multileader = self._make_multileader(style, dxfattribs)
        return MultiLeaderMTextBuilder(multileader)

    def add_multileader_block(
        self,
        style: str = "Standard",
        dxfattribs=None,
    ) -> MultiLeaderBlockBuilder:
        """Add a :class:`~ezdxf.entities.MultiLeader` entity but returns
        a :class:`~ezdxf.render.MultiLeaderBlockBuilder`.

        """
        from ezdxf.render.mleader import MultiLeaderBlockBuilder

        multileader = self._make_multileader(style, dxfattribs)
        return MultiLeaderBlockBuilder(multileader)

    def _make_multileader(
        self,
        style: str,
        dxfattribs=None,
    ) -> MultiLeader:
        if self.dxfversion < DXF2000:
            raise DXFVersionError("MULTILEADER requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        mleader_style = self.doc.mleader_styles.get(style)
        if mleader_style is None:
            raise DXFValueError(f"MLEADERSTYLE '{style}' does not exist")
        dxfattribs["style_handle"] = mleader_style.dxf.handle
        return self.new_entity("MULTILEADER", dxfattribs=dxfattribs)  # type: ignore

    def add_mline(
        self,
        vertices: Optional[Iterable[UVec]] = None,
        *,
        close: bool = False,
        dxfattribs=None,
    ) -> MLine:
        """Add a :class:`~ezdxf.entities.MLine` entity

        Args:
            vertices: MLINE vertices (in :ref:`WCS`)
            close: ``True`` to add a closed MLINE
            dxfattribs: additional DXF attributes

        """
        if self.dxfversion < DXF2000:
            raise DXFVersionError("MLine requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        style_name = dxfattribs.pop("style_name", "Standard")
        mline: MLine = self.new_entity("MLINE", dxfattribs)  # type: ignore
        # close() method regenerates geometry!
        mline.set_flag_state(mline.CLOSED, close)
        mline.set_style(style_name)
        if vertices:
            mline.extend(vertices)
        return mline

    def add_helix(
        self,
        radius: float,
        pitch: float,
        turns: float,
        ccw=True,
        dxfattribs=None,
    ) -> Helix:
        """
        Add a :class:`~ezdxf.entities.Helix` entity.

        The center of the helix is always (0, 0, 0) and the helix axis direction
        is the +z-axis.

        Transform the new HELIX by the :meth:`~ezdxf.entities.DXFGraphic.transform`
        method to your needs.

        Args:
            radius: helix radius
            pitch: the height of one complete helix turn
            turns: count of turns
            ccw: creates a counter-clockwise turning (right-handed) helix if ``True``
            dxfattribs: additional DXF attributes

        """
        from ezdxf import path

        if self.dxfversion < DXF2000:
            raise DXFVersionError("Helix requires DXF R2000")
        dxfattribs = dict(dxfattribs or {})
        helix: Helix = self.new_entity("HELIX", dxfattribs)  # type: ignore
        base = Vec3(0, 0, 0)
        helix.dxf.axis_base_point = base
        helix.dxf.radius = float(radius)
        helix.dxf.start_point = base + (radius, 0, 0)
        helix.dxf.axis_vector = Vec3(0, 0, 1 if pitch > 0 else -1)
        helix.dxf.turns = turns
        helix.dxf.turn_height = pitch
        helix.dxf.handedness = int(ccw)
        helix.dxf.constrain = 1  # turns
        p = path.helix(radius, pitch, turns, ccw)
        splines = list(path.to_bsplines_and_vertices(p))
        if splines:
            helix.apply_construction_tool(splines[0])
        return helix


LEADER_UNSUPPORTED_DIMSTYLE_ATTRIBS = {"dimblk", "dimblk1", "dimblk2"}
