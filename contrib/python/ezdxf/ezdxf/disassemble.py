#  Copyright (c) 2021-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterable, Optional, cast, TYPE_CHECKING
import abc
import math
from ezdxf.entities import DXFEntity, Insert, get_font_name

from ezdxf.lldxf import const
from ezdxf.enums import TextEntityAlignment
from ezdxf.math import Vec3, UCS, Z_AXIS, X_AXIS, BoundingBox
from ezdxf.path import Path, make_path, from_vertices, precise_bbox
from ezdxf.render import MeshBuilder, MeshVertexMerger, TraceBuilder
from ezdxf.protocols import SupportsVirtualEntities, virtual_entities

from ezdxf.tools.text import (
    TextLine,
    unified_alignment,
    plain_text,
    text_wrap,
    estimate_mtext_content_extents,
)
from ezdxf.fonts import fonts

if TYPE_CHECKING:
    from ezdxf.entities import LWPolyline, Polyline, MText, Text

__all__ = [
    "make_primitive",
    "recursive_decompose",
    "to_primitives",
    "to_vertices",
    "to_control_vertices",
    "to_paths",
    "to_meshes",
]


class Primitive(abc.ABC):
    """It is not efficient to create the Path() or MeshBuilder() representation
    by default. For some entities it's just not needed (LINE, POINT) and for
    others the builtin flattening() method is more efficient or accurate than
    using a Path() proxy object. (ARC, CIRCLE, ELLIPSE, SPLINE).

    The `max_flattening_distance` defines the max distance between the
    approximation line and the original curve. Use argument
    `max_flattening_distance` to override the default value, or set the value
    by direct attribute access.

    """

    max_flattening_distance: float = 0.01

    def __init__(
        self, entity: DXFEntity, max_flattening_distance: Optional[float] = None
    ):
        self.entity: DXFEntity = entity
        # Path representation for linear entities:
        self._path: Optional[Path] = None
        # MeshBuilder representation for mesh based entities:
        # PolygonMesh, PolyFaceMesh, Mesh
        self._mesh: Optional[MeshBuilder] = None
        if max_flattening_distance is not None:
            self.max_flattening_distance = max_flattening_distance

    @property
    def is_empty(self) -> bool:
        """Returns `True` if represents an empty primitive which do not
        yield any vertices.

        """
        if self._mesh:
            return len(self._mesh.vertices) == 0
        return self.path is None  # on demand calculations!

    @property
    def path(self) -> Optional[Path]:
        """:class:`~ezdxf.path.Path` representation or ``None``,
        idiom to check if is a path representation (could be empty)::

            if primitive.path is not None:
                process(primitive.path)

        """
        return None

    @property
    def mesh(self) -> Optional[MeshBuilder]:
        """:class:`~ezdxf.render.mesh.MeshBuilder` representation or ``None``,
        idiom to check if is a mesh representation (could be empty)::

            if primitive.mesh is not None:
                process(primitive.mesh)

        """
        return None

    @abc.abstractmethod
    def vertices(self) -> Iterable[Vec3]:
        """Yields all vertices of the path/mesh representation as
        :class:`~ezdxf.math.Vec3` objects.

        """
        pass

    def bbox(self, fast=False) -> BoundingBox:
        """Returns the :class:`~ezdxf.math.BoundingBox` of the path/mesh
        representation. Returns the precise bounding box for the path
        representation if `fast` is ``False``, otherwise the bounding box for
        Bézier curves is based on their control points.

        """
        if self.mesh:
            return BoundingBox(self.vertices())
        path = self.path
        if path:
            if fast:
                return BoundingBox(path.control_vertices())
            return precise_bbox(path)
        return BoundingBox()


class EmptyPrimitive(Primitive):
    @property
    def is_empty(self) -> bool:
        return True

    def vertices(self) -> Iterable[Vec3]:
        return []


class ConvertedPrimitive(Primitive):
    """Base class for all DXF entities which store the path/mesh representation
    at instantiation.

    """

    def __init__(self, entity: DXFEntity):
        super().__init__(entity)
        self._convert_entity()

    @abc.abstractmethod
    def _convert_entity(self):
        """This method creates the path/mesh representation."""
        pass

    @property
    def path(self) -> Optional[Path]:
        return self._path

    @property
    def mesh(self) -> Optional[MeshBuilder]:
        return self._mesh

    def vertices(self) -> Iterable[Vec3]:
        if self.path:
            yield from self._path.flattening(self.max_flattening_distance)  # type: ignore
        elif self.mesh:
            yield from self._mesh.vertices  # type: ignore


class CurvePrimitive(Primitive):
    @property
    def path(self) -> Optional[Path]:
        """Create path representation on demand."""
        if self._path is None:
            self._path = make_path(self.entity)
        return self._path

    def vertices(self) -> Iterable[Vec3]:
        # Not faster but more precise, because cubic bezier curves do not
        # perfectly represent elliptic arcs (CIRCLE, ARC, ELLIPSE).
        # SPLINE: cubic bezier curves do not perfectly represent splines with
        # degree != 3.
        yield from self.entity.flattening(self.max_flattening_distance)  # type: ignore


class LinePrimitive(Primitive):
    # TODO: apply thickness if not 0
    @property
    def path(self) -> Optional[Path]:
        """Create path representation on demand."""
        if self._path is None:
            self._path = make_path(self.entity)
        return self._path

    def vertices(self) -> Iterable[Vec3]:
        e = self.entity
        yield e.dxf.start
        yield e.dxf.end

    def bbox(self, fast=False) -> BoundingBox:
        e = self.entity
        return BoundingBox((e.dxf.start, e.dxf.end))


class LwPolylinePrimitive(ConvertedPrimitive):
    # TODO: apply thickness if not 0
    def _convert_entity(self) -> None:
        e: LWPolyline = cast("LWPolyline", self.entity)
        if e.has_width:  # use a mesh representation:
            # TraceBuilder operates in OCS!
            ocs = e.ocs()
            elevation = e.dxf.elevation
            tb = TraceBuilder.from_polyline(e)
            mb = MeshVertexMerger()  # merges coincident vertices
            for face in tb.faces_wcs(ocs, elevation):
                mb.add_face(face)
            self._mesh = MeshBuilder.from_builder(mb)
        else:  # use a path representation to support bulges!
            self._path = make_path(e)


class PointPrimitive(Primitive):
    @property
    def path(self) -> Optional[Path]:
        """Create path representation on demand.

        :class:`Path` can not represent a point, a :class:`Path` with only a
        start point yields not vertices!

        """
        if self._path is None:
            self._path = Path(self.entity.dxf.location)
        return self._path

    def vertices(self) -> Iterable[Vec3]:
        yield self.entity.dxf.location

    def bbox(self, fast=False) -> BoundingBox:
        return BoundingBox((self.entity.dxf.location,))


class MeshPrimitive(ConvertedPrimitive):
    def _convert_entity(self):
        self._mesh = MeshBuilder.from_mesh(self.entity)


class QuadrilateralPrimitive(ConvertedPrimitive):
    # TODO: apply thickness if not 0
    def _convert_entity(self):
        self._path = make_path(self.entity)


class PolylinePrimitive(ConvertedPrimitive):
    # TODO: apply thickness if not 0
    def _convert_entity(self) -> None:
        e: Polyline = cast("Polyline", self.entity)
        if e.is_2d_polyline and e.has_width:
            # TraceBuilder operates in OCS!
            ocs = e.ocs()
            elevation = e.dxf.elevation.z
            tb = TraceBuilder.from_polyline(e)
            mb = MeshVertexMerger()  # merges coincident vertices
            for face in tb.faces_wcs(ocs, elevation):
                mb.add_face(face)
            self._mesh = MeshBuilder.from_builder(mb)
        elif e.is_2d_polyline or e.is_3d_polyline:
            self._path = make_path(e)
        else:
            m = MeshVertexMerger.from_polyface(e)  # type: ignore
            self._mesh = MeshBuilder.from_builder(m)


class HatchPrimitive(ConvertedPrimitive):
    def _convert_entity(self):
        self._path = make_path(self.entity)


DESCENDER_FACTOR = 0.333  # from TXT SHX font - just guessing
X_HEIGHT_FACTOR = 0.666  # from TXT SHX font - just guessing


class TextLinePrimitive(ConvertedPrimitive):
    def _convert_entity(self) -> None:
        """Calculates the rough border path for a single line text.

        Calculation is based on a monospaced font and therefore the border
        path is just an educated guess.

        Vertical text generation and oblique angle is ignored.

        """

        def text_rotation():
            if fit_or_aligned and not p1.isclose(p2):
                return (p2 - p1).angle
            else:
                return math.radians(text.dxf.rotation)

        def location():
            if fit_or_aligned:
                return p1.lerp(p2, factor=0.5)
            return p1

        text = cast("Text", self.entity)
        if text.dxftype() == "ATTDEF":
            # ATTDEF outside of a BLOCK renders the tag rather than the value
            content = text.dxf.tag
        else:
            content = text.dxf.text

        content = plain_text(content)
        if len(content) == 0:
            # empty path - does not render any vertices!
            self._path = Path()
            return

        font = fonts.make_font(
            get_font_name(text), text.dxf.height, text.dxf.width
        )
        text_line = TextLine(content, font)
        alignment, p1, p2 = text.get_placement()
        if p2 is None:
            p2 = p1
        fit_or_aligned = (
            alignment == TextEntityAlignment.FIT
            or alignment == TextEntityAlignment.ALIGNED
        )
        if text.dxf.halign > 2:  # ALIGNED=3, MIDDLE=4, FIT=5
            text_line.stretch(alignment, p1, p2)
        halign, valign = unified_alignment(text)
        mirror_x = -1 if text.is_backward else 1
        mirror_y = -1 if text.is_upside_down else 1
        oblique: float = math.radians(text.dxf.oblique)
        corner_vertices = text_line.corner_vertices(
            location(),
            halign,
            valign,
            angle=text_rotation(),
            scale=(mirror_x, mirror_y),
            oblique=oblique,
        )

        ocs = text.ocs()
        self._path = from_vertices(
            ocs.points_to_wcs(corner_vertices),
            close=True,
        )


class MTextPrimitive(ConvertedPrimitive):
    def _convert_entity(self) -> None:
        """Calculates the rough border path for a MTEXT entity.

        Calculation is based on a mono-spaced font and therefore the border
        path is just an educated guess.

        Most special features of MTEXT is not supported.

        """

        def get_content() -> list[str]:
            text = mtext.plain_text(split=False)
            return text_wrap(text, box_width, font.text_width)  # type: ignore

        def get_max_str() -> str:
            return max(content, key=lambda s: len(s))

        def get_rect_width() -> float:
            if box_width:
                return box_width
            s = get_max_str()
            if len(s) == 0:
                s = " "
            return font.text_width(s)

        def get_rect_height() -> float:
            line_height = font.measurements.total_height
            cap_height = font.measurements.cap_height
            # Line spacing factor: Percentage of default (3-on-5) line
            # spacing to be applied.

            # thx to mbway: multiple of cap_height between the baseline of the
            # previous line and the baseline of the next line
            # 3-on-5 line spacing = 5/3 = 1.67
            line_spacing = cap_height * mtext.dxf.line_spacing_factor * 1.67
            spacing = line_spacing - line_height
            line_count = len(content)
            return line_height * line_count + spacing * (line_count - 1)

        def get_ucs() -> UCS:
            """Create local coordinate system:
            origin = insertion point
            z-axis = extrusion vector
            x-axis = text_direction or text rotation, text rotation requires
                extrusion vector == (0, 0, 1) or treatment like an OCS?

            """
            origin = mtext.dxf.insert
            z_axis = mtext.dxf.extrusion  # default is Z_AXIS
            x_axis = X_AXIS
            if mtext.dxf.hasattr("text_direction"):
                x_axis = mtext.dxf.text_direction
            elif mtext.dxf.hasattr("rotation"):
                # TODO: what if extrusion vector is not (0, 0, 1)
                x_axis = Vec3.from_deg_angle(mtext.dxf.rotation)
                z_axis = Z_AXIS
            return UCS(origin=origin, ux=x_axis, uz=z_axis)

        def get_shift_factors():
            halign, valign = unified_alignment(mtext)
            shift_x = 0
            shift_y = 0
            if halign == const.CENTER:
                shift_x = -0.5
            elif halign == const.RIGHT:
                shift_x = -1.0
            if valign == const.MIDDLE:
                shift_y = 0.5
            elif valign == const.BOTTOM:
                shift_y = 1.0
            return shift_x, shift_y

        def get_corner_vertices() -> Iterable[Vec3]:
            """Create corner vertices in the local working plan, where
            the insertion point is the origin.
            """
            if columns:
                rect_width = columns.total_width
                rect_height = columns.total_height
                if rect_width == 0.0 or rect_height == 0.0:
                    # Reliable sources like AutoCAD and BricsCAD do write
                    # correct total_width and total_height values!
                    w, h = _estimate_column_extents(mtext)
                    if rect_width == 0.0:
                        rect_width = w
                    if rect_height == 0.0:
                        rect_height = h
            else:
                rect_width = mtext.dxf.get("rect_width", get_rect_width())
                rect_height = mtext.dxf.get("rect_height", get_rect_height())
            # TOP LEFT alignment:
            vertices = [
                Vec3(0, 0),
                Vec3(rect_width, 0),
                Vec3(rect_width, -rect_height),
                Vec3(0, -rect_height),
            ]
            sx, sy = get_shift_factors()
            shift = Vec3(sx * rect_width, sy * rect_height)
            return (v + shift for v in vertices)

        mtext: MText = cast("MText", self.entity)
        columns = mtext.columns
        if columns is None:
            box_width = mtext.dxf.get("width", 0)
            font = fonts.make_font(
                get_font_name(mtext), mtext.dxf.char_height, 1.0
            )
            content: list[str] = get_content()
            if len(content) == 0:
                # empty path - does not render any vertices!
                self._path = Path()
                return
        ucs = get_ucs()
        corner_vertices = get_corner_vertices()
        self._path = from_vertices(
            ucs.points_to_wcs(corner_vertices),
            close=True,
        )


def _estimate_column_extents(mtext: MText):
    columns = mtext.columns
    assert columns is not None
    _content = mtext.text
    if columns.count > 1:
        _columns_content = _content.split("\\N")
        if len(_columns_content) > 1:
            _content = max(_columns_content, key=lambda t: len(t))
    return estimate_mtext_content_extents(
        content=_content,
        font=fonts.MonospaceFont(mtext.dxf.char_height, 1.0),
        column_width=columns.width,
        line_spacing_factor=mtext.dxf.get_default("line_spacing_factor"),
    )


class ImagePrimitive(ConvertedPrimitive):
    def _convert_entity(self):
        self._path = make_path(self.entity)


class ViewportPrimitive(ConvertedPrimitive):
    def _convert_entity(self):
        vp = self.entity
        if vp.dxf.status == 0:  # Viewport is off
            return  # empty primitive
        self._path = make_path(vp)


# SHAPE is not supported, could not create any SHAPE entities in BricsCAD
_PRIMITIVE_CLASSES = {
    "3DFACE": QuadrilateralPrimitive,
    "ARC": CurvePrimitive,
    # TODO: ATTRIB and ATTDEF could contain embedded MTEXT,
    #  but this is not supported yet!
    "ATTRIB": TextLinePrimitive,
    "ATTDEF": TextLinePrimitive,
    "CIRCLE": CurvePrimitive,
    "ELLIPSE": CurvePrimitive,
    "HATCH": HatchPrimitive,  # multi-path object
    "MPOLYGON": HatchPrimitive,  # multi-path object
    "HELIX": CurvePrimitive,
    "IMAGE": ImagePrimitive,
    "LINE": LinePrimitive,
    "LWPOLYLINE": LwPolylinePrimitive,
    "MESH": MeshPrimitive,
    "MTEXT": MTextPrimitive,
    "POINT": PointPrimitive,
    "POLYLINE": PolylinePrimitive,
    "SPLINE": CurvePrimitive,
    "SOLID": QuadrilateralPrimitive,
    "TEXT": TextLinePrimitive,
    "TRACE": QuadrilateralPrimitive,
    "VIEWPORT": ViewportPrimitive,
    "WIPEOUT": ImagePrimitive,
}


def make_primitive(
    entity: DXFEntity, max_flattening_distance=None
) -> Primitive:
    """Factory to create path/mesh primitives. The `max_flattening_distance`
    defines the max distance between the approximation line and the original
    curve. Use `max_flattening_distance` to override the default value.

    Returns an **empty primitive** for unsupported entities. The `empty` state
    of a primitive can be checked by the property :attr:`is_empty`.
    The :attr:`path` and the :attr:`mesh` attributes of an empty primitive
    are ``None`` and the :meth:`vertices` method  yields no vertices.

    """
    cls = _PRIMITIVE_CLASSES.get(entity.dxftype(), EmptyPrimitive)
    primitive = cls(entity)
    if max_flattening_distance:
        primitive.max_flattening_distance = max_flattening_distance
    return primitive


def recursive_decompose(entities: Iterable[DXFEntity]) -> Iterable[DXFEntity]:
    """Recursive decomposition of the given DXF entity collection into a flat
    stream of DXF entities. All block references (INSERT) and entities which provide
    a :meth:`virtual_entities` method will be disassembled into simple DXF
    sub-entities, therefore the returned entity stream does not contain any
    INSERT entity.

    Point entities will **not** be disassembled into DXF sub-entities,
    as defined by the current point style $PDMODE.

    These entity types include sub-entities and will be decomposed into
    simple DXF entities:

        - INSERT
        - DIMENSION
        - LEADER
        - MLEADER
        - MLINE

    Decomposition of XREF, UNDERLAY and ACAD_TABLE entities is not supported.

    This function does not apply the clipping path created by the XCLIP command. 
    The function returns all entities and ignores the clipping path polygon and no 
    entity is clipped.

    """
    for entity in entities:
        if isinstance(entity, Insert):
            # TODO: emit internal XCLIP marker entity?
            if entity.mcount > 1:
                yield from recursive_decompose(entity.multi_insert())
            else:
                yield from entity.attribs
                yield from recursive_decompose(virtual_entities(entity))
        # has a required __virtual_entities__() to be rendered?
        elif isinstance(entity, SupportsVirtualEntities):
            # could contain block references:
            yield from recursive_decompose(virtual_entities(entity))
        else:
            yield entity


def to_primitives(
    entities: Iterable[DXFEntity],
    max_flattening_distance: Optional[float] = None,
) -> Iterable[Primitive]:
    """Yields all DXF entities as path or mesh primitives. Yields
    unsupported entities as empty primitives, see :func:`make_primitive`.

    Args:
        entities: iterable of DXF entities
        max_flattening_distance: override the default value

    """
    for e in entities:
        yield make_primitive(e, max_flattening_distance)


def to_vertices(primitives: Iterable[Primitive]) -> Iterable[Vec3]:
    """Yields all vertices from the given `primitives`. Paths will be flattened
    to create the associated vertices. See also :func:`to_control_vertices` to
    collect only the control vertices from the paths without flattening.

    """
    for p in primitives:
        yield from p.vertices()


def to_paths(primitives: Iterable[Primitive]) -> Iterable[Path]:
    """Yields all :class:`~ezdxf.path.Path` objects from the given
    `primitives`. Ignores primitives without a defined path.

    """
    for prim in primitives:
        if prim.path is not None:  # lazy evaluation!
            yield prim.path


def to_meshes(primitives: Iterable[Primitive]) -> Iterable[MeshBuilder]:
    """Yields all :class:`~ezdxf.render.MeshBuilder` objects from the given
    `primitives`. Ignores primitives without a defined mesh.

    """
    for prim in primitives:
        if prim.mesh is not None:
            yield prim.mesh


def to_control_vertices(primitives: Iterable[Primitive]) -> Iterable[Vec3]:
    """Yields all path control vertices and all mesh vertices from the given
    `primitives`. Like :func:`to_vertices`, but without flattening.

    """
    for prim in primitives:
        # POINT has only a start point and yields from vertices()!
        if prim.path:
            yield from prim.path.control_vertices()
        else:
            yield from prim.vertices()
