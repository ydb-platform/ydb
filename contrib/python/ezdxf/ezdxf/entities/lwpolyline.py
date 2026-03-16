# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Tuple,
    Sequence,
    Iterable,
    Union,
    Iterator,
    Optional,
)
from typing_extensions import TypeAlias, Self
import array
import copy
from contextlib import contextmanager
from ezdxf.math import Vec3, Matrix44, Z_AXIS
from ezdxf.math.transformtools import OCSTransform, NonUniformScalingError
from ezdxf.lldxf import validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.lldxf.const import (
    SUBCLASS_MARKER,
    DXF2000,
    LWPOLYLINE_CLOSED,
    DXFStructureError,
)
from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.types import DXFTag, DXFVertex
from ezdxf.lldxf.packedtags import VertexArray
from ezdxf.render.polyline import virtual_lwpolyline_entities
from ezdxf.explode import explode_entity
from ezdxf.query import EntityQuery
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import DXFGraphic, acdb_entity
from .factory import register_entity
from .copy import default_copy

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace, Line, Arc, DXFEntity
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.layouts import BaseLayout

__all__ = ["LWPolyline", "FORMAT_CODES"]

LWPointType: TypeAlias = Tuple[float, float, float, float, float]

FORMAT_CODES = frozenset("xysebv")
DEFAULT_FORMAT = "xyseb"
LWPOINTCODES = (10, 20, 40, 41, 42)

# Order does matter:
# If tag 90 is not the first TAG, AutoCAD does not close the polyline, when the
# `close` flag is set.
acdb_lwpolyline = DefSubclass(
    "AcDbPolyline",
    {
        # Count always returns the actual length:
        "count": DXFAttr(90, xtype=XType.callback, getter="__len__"),
        # Elevation: OCS z-axis value for all vertices:
        "elevation": DXFAttr(38, default=0, optional=True),
        # Thickness can be negative!
        "thickness": DXFAttr(39, default=0, optional=True),
        # Flags:
        # 1 = Closed
        # 128 = Plinegen
        "flags": DXFAttr(70, default=0),
        # Const width: DXF reference error - AutoCAD uses just const width if not 0,
        # for all line segments.
        "const_width": DXFAttr(43, default=0, optional=True),
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            optional=True,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        # 10, 20 : Vertex x, y
        # 91: vertex identifier ???
        # 40, 41, 42: start width, end width, bulge
    },
)

acdb_lwpolyline_group_codes = group_code_mapping(acdb_lwpolyline)


@register_entity
class LWPolyline(DXFGraphic):
    """DXF LWPOLYLINE entity"""

    DXFTYPE = "LWPOLYLINE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_lwpolyline)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000

    def __init__(self):
        super().__init__()
        self.lwpoints = LWPolylinePoints()

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        """Copy lwpoints."""
        assert isinstance(entity, LWPolyline)
        entity.lwpoints = copy.deepcopy(self.lwpoints)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """
        Adds subclass processing for AcDbPolyline, requires previous base class
        and AcDbEntity processing by parent class.
        """
        dxf = super().load_dxf_attribs(processor)
        if processor:
            tags = processor.subclass_by_index(2)
            if tags:
                tags = self.load_vertices(tags)
                processor.fast_load_dxfattribs(
                    dxf,
                    acdb_lwpolyline_group_codes,
                    subclass=tags,
                    recover=True,
                )
            else:
                raise DXFStructureError(
                    f"missing 'AcDbPolyline' subclass in LWPOLYLINE(#{dxf.handle})"
                )
        return dxf

    def load_vertices(self, tags: Tags) -> Tags:
        self.lwpoints, unprocessed_tags = LWPolylinePoints.from_tags(tags)
        return unprocessed_tags

    def preprocess_export(self, tagwriter: AbstractTagWriter) -> bool:
        # Returns True if entity should be exported
        # Do not export polylines without vertices
        return len(self.lwpoints) > 0

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_lwpolyline.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            ["count", "flags", "const_width", "elevation", "thickness"],
        )
        tagwriter.write_tags(Tags(self.lwpoints.dxftags()))
        self.dxf.export_dxf_attribs(tagwriter, "extrusion")

    @property
    def closed(self) -> bool:
        """Get/set closed state of polyline.  A closed polyline has a connection
        segment from the last vertex to the first vertex.
        """
        return self.get_flag_state(LWPOLYLINE_CLOSED)

    @closed.setter
    def closed(self, status: bool) -> None:
        self.set_flag_state(LWPOLYLINE_CLOSED, status)

    @property
    def is_closed(self) -> bool:
        """Get closed state of LWPOLYLINE.
        Compatibility interface to :class:`Polyline`
        """
        return self.get_flag_state(LWPOLYLINE_CLOSED)

    def close(self, state: bool = True) -> None:
        """Set closed state of LWPOLYLINE.
        Compatibility interface to :class:`Polyline`
        """
        self.closed = state

    @property
    def has_arc(self) -> bool:
        """Returns ``True`` if LWPOLYLINE has an arc segment."""
        return any(bool(b) for x, y, s, e, b in self.lwpoints)

    @property
    def has_width(self) -> bool:
        """Returns ``True`` if LWPOLYLINE has any segment with width attributes
        or the DXF attribute const_width is not 0.

        """
        if self.dxf.hasattr("const_width"):
            # 'const_width' overrides all individual start- or end width settings.
            # The DXF reference claims the opposite, but that is simply not true.
            return self.dxf.const_width != 0.0
        return any((s or e) for x, y, s, e, b in self.lwpoints)

    def __len__(self) -> int:
        """Returns count of polyline points."""
        return len(self.lwpoints)

    def __iter__(self) -> Iterator[LWPointType]:
        """Returns iterable of tuples (x, y, start_width, end_width, bulge)."""
        return iter(self.lwpoints)

    def __getitem__(self, index: int) -> LWPointType:
        """Returns point at position `index` as (x, y, start_width, end_width,
        bulge) tuple. start_width, end_width and bulge is 0 if not present,
        supports extended slicing. Point format is fixed as "xyseb".

        All coordinates in :ref:`OCS`.

        """
        return self.lwpoints[index]

    def __setitem__(self, index: int, value: Sequence[float]) -> None:
        """
        Set point at position `index` as (x, y, [start_width, [end_width,
        [bulge]]]) tuple. If start_width or end_width is 0 or left off the
        default width value is used. If the bulge value is left off, bulge is 0
        by default (straight line).
        Does NOT support extend slicing. Point format is fixed as "xyseb".

        All coordinates in :ref:`OCS`.

        Args:
            index: point index
            value: point value as (x, y, [start_width, [end_width, [bulge]]]) tuple

        """
        self.lwpoints[index] = compile_array(value)

    def __delitem__(self, index: int) -> None:
        """Delete point at position `index`, supports extended slicing."""
        del self.lwpoints[index]

    def vertices(self) -> Iterator[tuple[float, float]]:
        """
        Returns iterable of all polyline points as (x, y) tuples in :ref:`OCS`
        (:attr:`dxf.elevation` is the z-axis value).

        """
        for point in self:
            yield point[0], point[1]

    def vertices_in_wcs(self) -> Iterator[Vec3]:
        """Returns iterable of all polyline points as Vec3(x, y, z) in :ref:`WCS`."""
        ocs = self.ocs()
        elevation = self.get_dxf_attrib("elevation", default=0.0)
        for x, y in self.vertices():
            yield ocs.to_wcs(Vec3(x, y, elevation))

    def vertices_in_ocs(self) -> Iterator[Vec3]:
        """Returns iterable of all polyline points as Vec3(x, y, z) in :ref:`OCS`."""
        elevation = self.get_dxf_attrib("elevation", default=0.0)
        for x, y in self.vertices():
            yield Vec3(x, y, elevation)

    def append(self, point: Sequence[float], format: str = DEFAULT_FORMAT) -> None:
        """Append `point` to polyline, `format` specifies a user defined
        point format.

        All coordinates in :ref:`OCS`.

        Args:
            point: (x, y, [start_width, [end_width, [bulge]]]) tuple
            format: format string, default is "xyseb", see: `format codes`_

        """
        self.lwpoints.append(point, format=format)

    def insert(
        self, pos: int, point: Sequence[float], format: str = DEFAULT_FORMAT
    ) -> None:
        """Insert new point in front of positions `pos`, `format` specifies a
        user defined point format.

        All coordinates in :ref:`OCS`.

        Args:
            pos: insert position
            point: point data
            format: format string, default is "xyseb", see: `format codes`_

        """
        data = compile_array(point, format=format)
        self.lwpoints.insert(pos, data)

    def append_points(
        self, points: Iterable[Sequence[float]], format: str = DEFAULT_FORMAT
    ) -> None:
        """
        Append new `points` to polyline, `format` specifies a user defined
        point format.

        All coordinates in :ref:`OCS`.

        Args:
            points: iterable of point, point is (x, y, [start_width, [end_width,
                [bulge]]]) tuple
            format: format string, default is "xyseb", see: `format codes`_

        """
        for point in points:
            self.lwpoints.append(point, format=format)

    @contextmanager
    def points(self, format: str = DEFAULT_FORMAT) -> Iterator[list[Sequence[float]]]:
        """Context manager for polyline points. Returns a standard Python list
        of points, according to the format string.

        All coordinates in :ref:`OCS`.

        Args:
            format: format string, see `format codes`_

        """
        points = self.get_points(format=format)
        yield points
        self.set_points(points, format=format)

    def get_points(self, format: str = DEFAULT_FORMAT) -> list[Sequence[float]]:
        """Returns all points as list of tuples, format specifies a user
        defined point format.

        All points in :ref:`OCS` as (x, y) tuples (:attr:`dxf.elevation` is
        the z-axis value).

        Args:
            format: format string, default is "xyseb", see `format codes`_

        """
        return [format_point(p, format=format) for p in self.lwpoints]

    def set_points(
        self, points: Iterable[Sequence[float]], format: str = DEFAULT_FORMAT
    ) -> None:
        """Remove all points and append new `points`.

        All coordinates in :ref:`OCS`.

        Args:
            points: iterable of point, point is (x, y, [start_width, [end_width,
                [bulge]]]) tuple
            format: format string, default is "xyseb", see `format codes`_

        """
        self.lwpoints.clear()
        self.append_points(points, format=format)

    def clear(self) -> None:
        """Remove all points."""
        self.lwpoints.clear()

    def transform(self, m: Matrix44) -> LWPolyline:
        """Transform the LWPOLYLINE entity by transformation matrix `m` inplace.

        A non-uniform scaling is not supported if the entity contains circular
        arc segments (bulges).

        Args:
            m: transformation :class:`~ezdxf.math.Matrix44`

        Raises:
            NonUniformScalingError: for non-uniform scaling of entity containing
                circular arc segments (bulges)

        """
        dxf = self.dxf
        ocs = OCSTransform(self.dxf.extrusion, m)
        if not ocs.scale_uniform and self.has_arc:
            raise NonUniformScalingError(
                "LWPOLYLINE containing arcs (bulges) does not support non uniform scaling"
            )
            # The caller function has to catch this exception and explode the
            # LWPOLYLINE into LINE and ELLIPSE entities.
        vertices = list(ocs.transform_vertex(v) for v in self.vertices_in_ocs())
        lwpoints = []
        for v, p in zip(vertices, self.lwpoints):
            _, _, start_width, end_width, bulge = p
            # assume a uniform scaling!
            start_width = ocs.transform_width(start_width)
            end_width = ocs.transform_width(end_width)
            lwpoints.append((v.x, v.y, start_width, end_width, bulge))
        self.set_points(lwpoints)

        # All new OCS vertices must have the same z-axis, which is the elevation
        # of the polyline:
        if vertices:
            dxf.elevation = vertices[0].z

        if dxf.hasattr("const_width"):  # assume a uniform scaling!
            dxf.const_width = ocs.transform_width(dxf.const_width)

        if dxf.hasattr("thickness"):
            dxf.thickness = ocs.transform_thickness(dxf.thickness)
        dxf.extrusion = ocs.new_extrusion
        self.post_transform(m)
        return self

    def virtual_entities(self) -> Iterator[Union[Line, Arc]]:
        """Yields the graphical representation of LWPOLYLINE as virtual DXF
        primitives (LINE or ARC).

        These virtual entities are located at the original location, but are not
        stored in the entity database, have no handle and are not assigned to
        any layout.

        """
        for e in virtual_lwpolyline_entities(self):
            e.set_source_of_copy(self)
            yield e

    def explode(self, target_layout: Optional[BaseLayout] = None) -> EntityQuery:
        """Explode the LWPOLYLINE entity as DXF primitives (LINE or ARC) into
        the target layout, if the target layout is ``None``, the target layout
        is the layout of the source entity. This method destroys the source entity.

        Returns an :class:`~ezdxf.query.EntityQuery` container referencing all DXF
        primitives.

        Args:
            target_layout: target layout for the DXF primitives, ``None`` for
                same layout as the source entity.

        """
        return explode_entity(self, target_layout)


class LWPolylinePoints(VertexArray):
    __slots__ = ("values",)
    VERTEX_CODE = 10
    START_WIDTH_CODE = 40
    END_WIDTH_CODE = 41
    BULGE_CODE = 42
    VERTEX_SIZE = 5

    @classmethod
    def from_tags(cls, tags: Iterable[DXFTag]) -> tuple[Self, Tags]:  # type: ignore
        """Setup point array from tags."""

        def build_vertex(point: list[float]) -> list[float]:
            point.append(attribs.get(cls.START_WIDTH_CODE, 0))
            point.append(attribs.get(cls.END_WIDTH_CODE, 0))
            point.append(attribs.get(cls.BULGE_CODE, 0))
            return point

        unprocessed_tags = Tags()
        vertices: list[Sequence[float]]= []
        point: list[float] | None = None
        attribs: dict[int, float]= {}
        for tag in tags:
            if tag.code in LWPOINTCODES:
                if tag.code == 10:
                    if point is not None:
                        vertices.append(build_vertex(point))
                    # just use x- and  y-axis
                    point = list(tag.value[0:2])
                    attribs = {}
                else:
                    attribs[tag.code] = tag.value
            else:
                unprocessed_tags.append(tag)
        if point is not None:
            vertices.append(build_vertex(point))
        return cls(data=vertices), unprocessed_tags

    def append(self, point: Sequence[float], format: str = DEFAULT_FORMAT) -> None:
        super().append(compile_array(point, format=format))

    def dxftags(self) -> Iterator[DXFTag]:
        for point in self:
            x, y, start_width, end_width, bulge = point
            yield DXFVertex(self.VERTEX_CODE, (x, y))
            if start_width or end_width:
                # Export always start- and end width together,
                # required for BricsCAD but not AutoCAD!
                yield DXFTag(self.START_WIDTH_CODE, start_width)
                yield DXFTag(self.END_WIDTH_CODE, end_width)
            if bulge:
                yield DXFTag(self.BULGE_CODE, bulge)


def format_point(point: Sequence[float], format: str = "xyseb") -> Sequence[float]:
    """Reformat point components.

    Format codes:

        - ``x`` = x-coordinate
        - ``y`` = y-coordinate
        - ``s`` = start width
        - ``e`` = end width
        - ``b`` = bulge value
        - ``v`` = (x, y) as tuple

    Args:
        point: list or tuple of (x, y, start_width, end_width, bulge)
        format: format string, default is "xyseb"

    Returns:
        Sequence[float]: tuple of selected components

    """
    x, y, s, e, b = point
    v = (x, y)
    vars = locals()
    return tuple(vars[code] for code in format.lower() if code in FORMAT_CODES)


def compile_array(data: Sequence[float], format="xyseb") -> array.array:
    """Gather point components from input data.

    Format codes:

        - ``x`` = x-coordinate
        - ``y`` = y-coordinate
        - ``s`` = start width
        - ``e`` = end width
        - ``b`` = bulge value
        - ``v`` = (x, y [,z]) tuple (z-axis is ignored)

    Args:
        data: list or tuple of point components
        format: format string, default is "xyseb"

    Returns:
        array.array: array.array('d', (x, y, start_width, end_width, bulge))

    """
    a = array.array("d", (0.0, 0.0, 0.0, 0.0, 0.0))
    format = [code for code in format.lower() if code in FORMAT_CODES]
    for code, value in zip(format, data):
        if code not in FORMAT_CODES:
            continue
        if code == "v":
            vertex = Vec3(value)
            a[0] = vertex.x
            a[1] = vertex.y
        else:
            a["xyseb".index(code)] = value
    return a
