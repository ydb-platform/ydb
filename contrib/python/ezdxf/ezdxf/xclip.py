# Copyright (c) 2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence
import dataclasses

from ezdxf.lldxf import const
from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.types import dxftag
from ezdxf.entities import SpatialFilter, DXFEntity, Dictionary, Insert, XRecord
from ezdxf.math import Vec2, Vec3, UVec, Z_AXIS, Matrix44, BoundingBox2d
from ezdxf.entities.acad_xrec_roundtrip import RoundtripXRecord

__all__ = ["get_spatial_filter", "XClip", "ClippingPath"]

ACAD_FILTER = "ACAD_FILTER"
ACAD_XREC_ROUNDTRIP = "ACAD_XREC_ROUNDTRIP"
ACAD_INVERTEDCLIP_ROUNDTRIP = "ACAD_INVERTEDCLIP_ROUNDTRIP"
ACAD_INVERTEDCLIP_ROUNDTRIP_COMPARE = "ACAD_INVERTEDCLIP_ROUNDTRIP_COMPARE"

SPATIAL = "SPATIAL"


@dataclasses.dataclass
class ClippingPath:
    """Stores the SPATIAL_FILTER clipping paths in original form that I still don't fully
    understand for `inverted` clipping paths. All boundary paths are simple polygons as a
    sequence of :class:`~ezdxf.math.Vec2`.

    Attributes:
        vertices: Contains the boundary polygon for regular clipping paths.
            Contains the outer boundary path for inverted clippings paths - but not always!
        inverted_clip:
            Contains the inner boundary for inverted clipping paths - but not always!
        inverted_clip_compare:
            Contains the combined inner- and the outer boundaries for inverted
            clipping paths - but not always!
        is_inverted_clip: ``True`` for inverted clipping paths

    """

    vertices: Sequence[Vec2] = tuple()
    inverted_clip: Sequence[Vec2] = tuple()
    inverted_clip_compare: Sequence[Vec2] = tuple()
    is_inverted_clip: bool = False

    def inner_polygon(self) -> Sequence[Vec2]:
        """Returns the inner clipping polygon as sequence of Vec2."""
        # The exact data structure of inverted clippings polygons is still not
        # clear to me, so use the smallest polygon as inner clipping polygon.
        if not self.is_inverted_clip:
            return self.vertices
        inner_polygon = self.vertices
        if bbox_area(self.inverted_clip) < bbox_area(inner_polygon):
            inner_polygon = self.inverted_clip
        return inner_polygon

    def outer_bounds(self) -> BoundingBox2d:
        """Returns the maximum extents as BoundingBox2d."""
        if not self.is_inverted_clip:
            return BoundingBox2d(self.vertices)
        # The exact data structure of inverted clippings polygons is still not
        # clear to me, this is my best guess:
        return BoundingBox2d(self.inverted_clip_compare)


class XClip:
    """Helper class to manage the clipping path of INSERT entities.

    Provides a similar functionality as the XCLIP command in CAD applications.

    .. important::

        This class handles only 2D clipping paths.

    The visibility of the clipping path can be set individually for each block
    reference, but the HEADER variable $XCLIPFRAME ultimately determines whether the
    clipping path is displayed or plotted by the application:

    === =============== ===
    0   not displayed   not plotted
    1   displayed       not plotted
    2   displayed       plotted
    === =============== ===

    The default setting is 2.

    """

    def __init__(self, insert: Insert) -> None:
        if not isinstance(insert, Insert):
            raise const.DXFTypeError(f"INSERT entity required, got {str(insert)}")
        self._insert = insert
        self._spatial_filter = get_spatial_filter(insert)

    def get_spatial_filter(self) -> SpatialFilter | None:
        """Returns the underlaying SPATIAL_FILTER entity if the INSERT entity has a
        clipping path and returns ``None`` otherwise.
        """
        return self._spatial_filter

    def get_xclip_frame_policy(self) -> int:
        policy: int = 2
        if self._insert.doc is not None:
            policy = self._insert.doc.header.get("$XCLIPFRAME", 2)
        return policy

    @property
    def has_clipping_path(self) -> bool:
        """Returns if the INSERT entity has a clipping path."""
        return self._spatial_filter is not None

    @property
    def is_clipping_enabled(self) -> bool:
        """Returns ``True`` if block reference clipping is enabled."""
        if isinstance(self._spatial_filter, SpatialFilter):
            return bool(self._spatial_filter.dxf.is_clipping_enabled)
        return False

    @property
    def is_inverted_clip(self) -> bool:
        """Returns ``True`` if clipping path is inverted."""
        xrec = get_roundtrip_xrecord(self._spatial_filter)
        if xrec is None:
            return False
        return xrec.has_section(ACAD_INVERTEDCLIP_ROUNDTRIP)

    def enable_clipping(self) -> None:
        """Enable block reference clipping."""
        if isinstance(self._spatial_filter, SpatialFilter):
            self._spatial_filter.dxf.is_clipping_enabled = 1

    def disable_clipping(self) -> None:
        """Disable block reference clipping."""
        if isinstance(self._spatial_filter, SpatialFilter):
            self._spatial_filter.dxf.is_clipping_enabled = 0

    def get_block_clipping_path(self) -> ClippingPath:
        """Returns the clipping path in block coordinates (relative to the block origin)."""
        vertices: Sequence[Vec2] = []
        if not isinstance(self._spatial_filter, SpatialFilter):
            return ClippingPath()
        m = self._spatial_filter.inverse_insert_matrix
        vertices = Vec2.tuple(
            m.transform_vertices(self._spatial_filter.boundary_vertices)
        )
        if len(vertices) == 2:
            vertices = _rect_path(vertices)

        clipping_path = ClippingPath(vertices, is_inverted_clip=False)
        xrec = get_roundtrip_xrecord(self._spatial_filter)
        if isinstance(xrec, RoundtripXRecord):
            clipping_path.inverted_clip = get_roundtrip_vertices(
                xrec, ACAD_INVERTEDCLIP_ROUNDTRIP, m
            )
            clipping_path.inverted_clip_compare = get_roundtrip_vertices(
                xrec, ACAD_INVERTEDCLIP_ROUNDTRIP_COMPARE, m
            )
            clipping_path.is_inverted_clip = True
        return clipping_path

    def get_wcs_clipping_path(self) -> ClippingPath:
        """Returns the clipping path in WCS coordinates (relative to the WCS origin) as
        2D path projected onto the xy-plane.
        """
        vertices: Sequence[Vec2] = tuple()
        if not isinstance(self._spatial_filter, SpatialFilter):
            return ClippingPath(vertices, vertices)
        block_clipping_path = self.get_block_clipping_path()
        m = self._insert.matrix44()
        vertices = Vec2.tuple(m.transform_vertices(block_clipping_path.vertices))
        if len(vertices) == 2:  # rectangle by diagonal corner vertices
            vertices = BoundingBox2d(vertices).rect_vertices()
        wcs_clipping_path = ClippingPath(
            vertices, is_inverted_clip=block_clipping_path.is_inverted_clip
        )
        if block_clipping_path.is_inverted_clip:
            inverted_clip = Vec2.tuple(
                m.transform_vertices(block_clipping_path.inverted_clip)
            )
            if len(inverted_clip) == 2:  # rectangle by diagonal corner vertices
                inverted_clip = BoundingBox2d(inverted_clip).rect_vertices()
            wcs_clipping_path.inverted_clip = inverted_clip
            wcs_clipping_path.inverted_clip_compare = Vec2.tuple(
                m.transform_vertices(block_clipping_path.inverted_clip_compare)
            )
        return wcs_clipping_path

    def set_block_clipping_path(self, vertices: Iterable[UVec]) -> None:
        """Set clipping path in block coordinates (relative to block origin).

        The clipping path is located in the xy-plane, the z-axis of all vertices will
        be ignored.  The clipping path doesn't have to be closed (first vertex != last vertex).
        Two vertices define a rectangle where the sides are parallel to x- and y-axis.

        Raises:
           DXFValueError: clipping path has less than two vertrices

        """
        if self._spatial_filter is None:
            self._spatial_filter = new_spatial_filter(self._insert)
        spatial_filter = self._spatial_filter
        spatial_filter.set_boundary_vertices(vertices)
        spatial_filter.dxf.origin = Vec3(0, 0, 0)
        spatial_filter.dxf.extrusion = Z_AXIS
        spatial_filter.dxf.has_front_clipping_plane = 0
        spatial_filter.dxf.front_clipping_plane_distance = 0.0
        spatial_filter.dxf.has_back_clipping_plane = 0
        spatial_filter.dxf.back_clipping_plane_distance = 0.0

        # The clipping path set by ezdxf is always relative to the block origin and
        # therefore both transformation matrices are the identity matrix - which does
        # nothing.
        m = Matrix44()
        spatial_filter.set_inverse_insert_matrix(m)
        spatial_filter.set_transform_matrix(m)
        self._discard_inverted_clip()

    def set_wcs_clipping_path(self, vertices: Iterable[UVec]) -> None:
        """Set clipping path in WCS coordinates (relative to WCS origin).

        The clipping path is located in the xy-plane, the z-axis of all vertices will
        be ignored. The clipping path doesn't have to be closed (first vertex != last vertex).
        Two vertices define a rectangle where the sides are parallel to x- and y-axis.

        Raises:
           DXFValueError: clipping path has less than two vertrices
           ZeroDivisionError: Block reference transformation matrix is not invertible

        """
        m = self._insert.matrix44()
        try:
            m.inverse()
        except ZeroDivisionError:
            raise ZeroDivisionError(
                "Block reference transformation matrix is not invertible."
            )
        _vertices = Vec2.list(vertices)
        if len(_vertices) == 2:
            _vertices = _rect_path(_vertices)
        self.set_block_clipping_path(m.transform_vertices(_vertices))

    def invert_clipping_path(
        self, extents: Iterable[UVec] | None = None, *, ignore_acad_compatibility=False
    ) -> None:
        """Invert clipping path. (experimental feature)

        The outer boundary is defined by the bounding box of the given `extents`
        vertices or auto-detected if `extents` is ``None``.

        The `extents` are BLOCK coordinates.
        Requires an existing clipping path and that clipping path cannot be inverted.

        .. warning::

            You have to set the flag `ignore_acad_compatibility` to ``True`` to use
            this feature.  AutoCAD will not load DXF files with inverted clipping paths
            created by ezdxf!!!!

        """
        if ignore_acad_compatibility is False:
            return

        current_clipping_path = self.get_block_clipping_path()
        if len(current_clipping_path.vertices) < 2:
            raise const.DXFValueError("no clipping path set")
        if current_clipping_path.is_inverted_clip:
            raise const.DXFValueError("clipping path is already inverted")

        assert self._insert.doc is not None
        self._insert.doc.add_acad_incompatibility_message(
            "\nAutoCAD will not load DXF files with inverted clipping paths created by ezdxf"
        )
        grow_factor = 0.0
        if extents is None:
            extents = self._detect_block_extents()
            # grow bounding box by 10%, bbox detection is not very precise for text
            # based entities:
            grow_factor = 0.1

        bbox = BoundingBox2d(extents)
        bbox.extend(current_clipping_path.vertices)
        if not bbox.has_data:
            raise const.DXFValueError("extents not detectable")

        if grow_factor:
            bbox.grow(max(bbox.size) * grow_factor)

        # inverted_clip is the regular clipping path
        inverted_clip = current_clipping_path.vertices
        # construct an inverted clipping path
        inverted_clip_compare = _get_inverted_clip_compare_vertices(bbox, inverted_clip)
        # set inverted_clip_compare as regular clipping path
        self.set_block_clipping_path(inverted_clip_compare)
        self._set_inverted_clipping_path(inverted_clip, inverted_clip_compare)

    def _detect_block_extents(self) -> Sequence[Vec2]:
        from ezdxf import bbox

        insert = self._insert
        doc = insert.doc
        assert doc is not None, "valid DXF document required"
        no_vertices: Sequence[Vec2] = tuple()
        block = doc.blocks.get(insert.dxf.name)
        if block is None:
            return no_vertices

        _bbox = bbox.extents(block, fast=True)
        if not _bbox.has_data:
            return no_vertices
        return Vec2.tuple([_bbox.extmin, _bbox.extmax])

    def _set_inverted_clipping_path(
        self, clip_vertices: Iterable[Vec2], compare_vertices: Iterable[Vec2]
    ) -> None:
        spatial_filter = self._spatial_filter
        assert isinstance(spatial_filter, SpatialFilter)
        xrec = get_roundtrip_xrecord(spatial_filter)
        if xrec is None:
            xrec = new_roundtrip_xrecord(spatial_filter)

        clip_tags = Tags(dxftag(10, Vec3(v)) for v in clip_vertices)
        compare_tags = Tags(dxftag(10, Vec3(v)) for v in compare_vertices)
        xrec.set_section(ACAD_INVERTEDCLIP_ROUNDTRIP, clip_tags)
        xrec.set_section(ACAD_INVERTEDCLIP_ROUNDTRIP_COMPARE, compare_tags)

    def discard_clipping_path(self) -> None:
        """Delete the clipping path. The clipping path doesn't have to exist.

        This method does not discard the extension dictionary of the base entity,
        even when its empty.
        """
        if not isinstance(self._spatial_filter, SpatialFilter):
            return

        xdict = self._insert.get_extension_dict()
        xdict.discard(ACAD_FILTER)
        entitydb = self._insert.doc.entitydb  # type: ignore
        assert entitydb is not None
        entitydb.delete_entity(self._spatial_filter)
        self._spatial_filter = None

    def _discard_inverted_clip(self) -> None:
        if isinstance(self._spatial_filter, SpatialFilter):
            self._spatial_filter.discard_extension_dict()

    def cleanup(self):
        """Discard the extension dictionary of the base entity when empty."""
        self._insert.discard_empty_extension_dict()


def _rect_path(vertices: Iterable[Vec2]) -> Sequence[Vec2]:
    """Returns the path vertices for the smallest rectangular boundary around the given
    vertices.
    """
    return BoundingBox2d(vertices).rect_vertices()


def _get_inverted_clip_compare_vertices(
    bbox: BoundingBox2d, hole: Sequence[Vec2]
) -> Sequence[Vec2]:
    # AutoCAD does not accept this paths and from further tests it's clear that the 
    # geometry of the inverted clipping path is the problem not the DXF structure!
    from ezdxf.math.clipping import make_inverted_clipping_polygon

    assert (bbox.extmax is not None) and (bbox.extmin is not None)
    return make_inverted_clipping_polygon(inner_polygon=list(hole), outer_bounds=bbox)


def get_spatial_filter(entity: DXFEntity) -> SpatialFilter | None:
    """Returns the underlaying SPATIAL_FILTER entity if the given `entity` has a
    clipping path and returns ``None`` otherwise.
    """
    try:
        xdict = entity.get_extension_dict()
    except AttributeError:
        return None
    acad_filter = xdict.get(ACAD_FILTER)
    if not isinstance(acad_filter, Dictionary):
        return None
    acad_spatial_filter = acad_filter.get(SPATIAL)
    if isinstance(acad_spatial_filter, SpatialFilter):
        return acad_spatial_filter
    return None


def new_spatial_filter(entity: DXFEntity) -> SpatialFilter:
    """Creates the extension dict, the sub-dictionary ACAD_FILTER and the SPATIAL_FILTER
    entity if not exist.
    """
    doc = entity.doc
    if doc is None:
        raise const.DXFTypeError("Cannot add new clipping path to virtual entity.")
    try:
        xdict = entity.get_extension_dict()
    except AttributeError:
        xdict = entity.new_extension_dict()
    acad_filter_dict = xdict.dictionary.get_required_dict(ACAD_FILTER, hard_owned=True)
    spatial_filter = acad_filter_dict.get(SPATIAL)
    if not isinstance(spatial_filter, SpatialFilter):
        spatial_filter = doc.objects.add_dxf_object_with_reactor(
            "SPATIAL_FILTER", {"owner": acad_filter_dict.dxf.handle}
        )
        acad_filter_dict.add(SPATIAL, spatial_filter)
    assert isinstance(spatial_filter, SpatialFilter)
    return spatial_filter


def new_roundtrip_xrecord(spatial_filter: SpatialFilter) -> RoundtripXRecord:
    try:
        xdict = spatial_filter.get_extension_dict()
    except AttributeError:
        xdict = spatial_filter.new_extension_dict()
    xrec = xdict.get(ACAD_XREC_ROUNDTRIP)
    if xrec is None:
        xrec = xdict.add_xrecord(ACAD_XREC_ROUNDTRIP)
        xrec.set_reactors([xdict.handle])
    assert isinstance(xrec, XRecord)
    return RoundtripXRecord(xrec)


def get_roundtrip_xrecord(
    spatial_filter: SpatialFilter | None,
) -> RoundtripXRecord | None:
    if spatial_filter is None:
        return None
    try:
        xdict = spatial_filter.get_extension_dict()
    except AttributeError:
        return None
    xrecord = xdict.get(ACAD_XREC_ROUNDTRIP)
    if isinstance(xrecord, XRecord):
        return RoundtripXRecord(xrecord)
    return None


def get_roundtrip_vertices(
    xrec: RoundtripXRecord, section_name: str, m: Matrix44
) -> Sequence[Vec2]:
    tags = xrec.get_section(section_name)
    vertices = m.transform_vertices(Vec3(t.value) for t in tags)
    return Vec2.tuple(vertices)


def bbox_area(vertice: Sequence[Vec2]) -> float:
    bbox = BoundingBox2d(vertice)
    if bbox.has_data:
        size = bbox.size
        return size.x * size.y
    return 0.0
