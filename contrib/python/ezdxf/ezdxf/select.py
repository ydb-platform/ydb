# Copyright (c) 2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Callable, Sequence
from typing_extensions import override
import abc

from ezdxf import bbox
from ezdxf.entities import DXFEntity
from ezdxf.math import UVec, Vec2, Vec3, BoundingBox2d, is_point_in_polygon_2d
from ezdxf.math.clipping import CohenSutherlandLineClipping2d
from ezdxf.math import rtree, BoundingBox
from ezdxf.query import EntityQuery


__all__ = [
    "bbox_chained",
    "bbox_crosses_fence",
    "bbox_inside",
    "bbox_outside",
    "bbox_overlap",
    "Circle",
    "PlanarSearchIndex",
    "point_in_bbox",
    "Polygon",
    "Window",
]


class SelectionShape(abc.ABC):
    """AbstractBaseClass for selection shapes.

    It is guaranteed that all methods get an entity_bbox which has data!
    """

    @abc.abstractmethod
    def is_inside_bbox(self, entity_bbox: BoundingBox2d) -> bool: ...

    @abc.abstractmethod
    def is_outside_bbox(self, entity_bbox: BoundingBox2d) -> bool: ...

    @abc.abstractmethod
    def is_overlapping_bbox(self, entity_bbox: BoundingBox2d) -> bool: ...


class Window(SelectionShape):
    """This selection shape tests entities against a rectangular and axis-aligned 2D
    window.  All entities are projected on the xy-plane.

    Args:
        p1: first corner of the window
        p2: second corner of the window
    """

    def __init__(self, p1: UVec, p2: UVec):
        self._bbox = BoundingBox2d((p1, p2))

    @override
    def is_inside_bbox(self, entity_bbox: BoundingBox2d) -> bool:
        return self._bbox.contains(entity_bbox)

    @override
    def is_outside_bbox(self, entity_bbox: BoundingBox2d) -> bool:
        return not self._bbox.has_overlap(entity_bbox)

    @override
    def is_overlapping_bbox(self, entity_bbox: BoundingBox2d) -> bool:
        return self._bbox.has_overlap(entity_bbox)


class Circle(SelectionShape):
    """This selection shape tests entities against a circle.  All entities are
    projected on the xy-plane.

    Args:
        center: center of the circle
        radius: radius of the circle
    """

    def __init__(self, center: UVec, radius: float):
        self._center = Vec2(center)
        self._radius = float(radius)
        r_vec = Vec2(self._radius, self._radius)
        self._bbox = BoundingBox2d((self._center - r_vec, self._center + r_vec))

    def _is_vertex_inside(self, v: Vec2) -> bool:
        return self._center.distance(v) <= self._radius

    @override
    def is_inside_bbox(self, entity_bbox: BoundingBox2d) -> bool:
        return all(self._is_vertex_inside(v) for v in entity_bbox.rect_vertices())

    @override
    def is_outside_bbox(self, entity_bbox: BoundingBox2d) -> bool:
        return not self.is_overlapping_bbox(entity_bbox)

    @override
    def is_overlapping_bbox(self, entity_bbox: BoundingBox2d) -> bool:
        if not self._bbox.has_overlap(entity_bbox):
            return False
        if any(self._is_vertex_inside(v) for v in entity_bbox.rect_vertices()):
            return True
        return self._is_vertex_inside(entity_bbox.center)


class Polygon(SelectionShape):
    """This selection shape tests entities against an arbitrary closed polygon.
    All entities are projected on the xy-plane. Complex **concave** polygons may not
    work as expected.
    """

    def __init__(self, vertices: Iterable[UVec]):
        v = Vec2.list(vertices)
        if len(v) < 3:
            raise ValueError("3 or more vertices required")
        if v[0].isclose(v[-1]):
            v.pop()  # open polygon
        if len(v) < 3:
            raise ValueError("3 or more vertices required")
        self._vertices: list[Vec2] = v
        self._bbox = BoundingBox2d(self._vertices)

    def _has_intersection(self, extmin: Vec2, extmax: Vec2) -> bool:
        cs = CohenSutherlandLineClipping2d(extmin, extmax)
        vertices = self._vertices
        for index, end in enumerate(vertices):
            if cs.clip_line(vertices[index - 1], end):
                return True
        return False

    @override
    def is_inside_bbox(self, entity_bbox: BoundingBox2d) -> bool:
        if not self._bbox.has_overlap(entity_bbox):
            return False
        if any(
            is_point_in_polygon_2d(v, self._vertices) < 0  # outside
            for v in entity_bbox.rect_vertices()
        ):
            return False

        # Additional test for concave polygons. This may not cover all concave polygons.
        # Is any point of the polygon (strict) inside the entity bbox?
        min_x, min_y = entity_bbox.extmin
        max_x, max_y = entity_bbox.extmax
        # strict inside test: points on the boundary line do not count as inside
        return not any(
            (min_x < v.x < max_x) and (min_y < v.y < max_y) for v in self._vertices
        )

    @override
    def is_outside_bbox(self, entity_bbox: BoundingBox2d) -> bool:
        return not self.is_overlapping_bbox(entity_bbox)

    @override
    def is_overlapping_bbox(self, entity_bbox: BoundingBox2d) -> bool:
        if not self._bbox.has_overlap(entity_bbox):
            return False
        if any(
            is_point_in_polygon_2d(v, self._vertices) >= 0  # inside or on boundary
            for v in entity_bbox.rect_vertices()
        ):
            return True
        # special case: all bbox corners are outside the polygon but bbox edges may
        # intersect the polygon
        return self._has_intersection(entity_bbox.extmin, entity_bbox.extmax)


def bbox_inside(
    shape: SelectionShape,
    entities: Iterable[DXFEntity],
    *,
    cache: bbox.Cache | None = None,
) -> EntityQuery:
    """Selects entities whose bounding box lies withing the selection shape.

    Args:
        shape: seclection shape
        entities: iterable of DXFEntities
        cache: optional :class:`ezdxf.bbox.Cache` instance

    """
    return select_by_bbox(entities, shape.is_inside_bbox, cache)


def bbox_outside(
    shape: SelectionShape,
    entities: Iterable[DXFEntity],
    *,
    cache: bbox.Cache | None = None,
) -> EntityQuery:
    """Selects entities whose bounding box is completely outside the selection shape.

    Args:
        shape: seclection shape
        entities: iterable of DXFEntities
        cache: optional :class:`ezdxf.bbox.Cache` instance

    """
    return select_by_bbox(entities, shape.is_outside_bbox, cache)


def bbox_overlap(
    shape: SelectionShape,
    entities: Iterable[DXFEntity],
    *,
    cache: bbox.Cache | None = None,
) -> EntityQuery:
    """Selects entities whose bounding box overlaps the selection shape.

    Args:
        shape: seclection shape
        entities: iterable of DXFEntities
        cache: optional :class:`ezdxf.bbox.Cache` instance

    """
    return select_by_bbox(entities, shape.is_overlapping_bbox, cache)


def select_by_bbox(
    entities: Iterable[DXFEntity],
    test_func: Callable[[BoundingBox2d], bool],
    cache: bbox.Cache | None = None,
) -> EntityQuery:
    """Calculates the bounding box for each entity and returns all entities for that the
    test function returns ``True``.

    Args:
        entities: iterable of DXFEntities
        func: test function which takes the bounding box of the entity as input and
            returns ``True`` if the entity is part of the selection.
        cache: optional :class:`ezdxf.bbox.Cache` instance

    """
    selection: list[DXFEntity] = []

    for entity in entities:
        extents = bbox.extents((entity,), fast=True, cache=cache)
        if not extents.has_data:
            continue
        if test_func(BoundingBox2d(extents)):
            selection.append(entity)
    return EntityQuery(selection)


def bbox_crosses_fence(
    vertices: Iterable[UVec],
    entities: Iterable[DXFEntity],
    *,
    cache: bbox.Cache | None = None,
) -> EntityQuery:
    """Selects entities whose bounding box intersects an open polyline.

    All entities are projected on the xy-plane.

    A single point can not be selected by a fence polyline by definition.

    Args:
        vertices: vertices of the selection polyline
        entities: iterable of DXFEntities
        cache: optional :class:`ezdxf.bbox.Cache` instance

    """

    def is_crossing(entity_bbox: BoundingBox2d) -> bool:
        if not _bbox.has_overlap(entity_bbox):
            return False
        if any(entity_bbox.inside(v) for v in _vertices):
            return True
        # All fence vertices are outside the entity bbox, but fence edges may
        # intersect the entity bbox.
        extmin = entity_bbox.extmin
        extmax = entity_bbox.extmax
        if extmin.isclose(extmax):  # is point
            return False  # by definition
        cs = CohenSutherlandLineClipping2d(extmin, extmax)
        return any(
            cs.clip_line(start, end) for start, end in zip(_vertices, _vertices[1:])
        )

    _vertices = Vec2.list(vertices)
    if len(_vertices) < 2:
        raise ValueError("2 or more vertices required")
    _bbox = BoundingBox2d(_vertices)

    return select_by_bbox(entities, is_crossing, cache)


def point_in_bbox(
    location: UVec, entities: Iterable[DXFEntity], *, cache: bbox.Cache | None = None
) -> EntityQuery:
    """Selects entities where the selection point lies within the bounding box.
    All entities are projected on the xy-plane.

    Args:
        point: selection point
        entities: iterable of DXFEntities
        cache: optional :class:`ezdxf.bbox.Cache` instance

    """

    def is_crossing(entity_bbox: BoundingBox2d) -> bool:
        return entity_bbox.inside(point)

    point = Vec2(location)
    return select_by_bbox(entities, is_crossing, cache)


def bbox_chained(
    start: DXFEntity, entities: Iterable[DXFEntity], *, cache: bbox.Cache | None = None
) -> EntityQuery:
    """Selects elements that are directly or indirectly connected to each other by
    overlapping bounding boxes. The selection begins at the specified starting element.

    Warning: the current implementation has a complexity of O(n²).

    Args:
        start: first entity of selection
        entities: iterable of DXFEntities
        cache: optional :class:`ezdxf.bbox.Cache` instance

    """

    def get_bbox_2d(entity: DXFEntity) -> BoundingBox2d:
        return BoundingBox2d(bbox.extents((entity,), fast=True, cache=cache))

    if cache is None:
        cache = bbox.Cache()
    selected: dict[DXFEntity, BoundingBox2d] = {start: get_bbox_2d(start)}

    entities = list(entities)
    restart = True
    while restart:
        restart = False
        for entity in entities:
            if entity in selected:
                continue
            entity_bbox = get_bbox_2d(entity)
            for selected_bbox in selected.values():
                if entity_bbox.has_overlap(selected_bbox):
                    selected[entity] = entity_bbox
                    restart = True
                    break

    return EntityQuery(selected.keys())


class PlanarSearchIndex:
    """**Spatial Search Index for DXF Entities**

    This class implements a spatial search index for DXF entities based on their
    bounding boxes except for POINT and LINE.
    It operates strictly within the two-dimensional (2D) space of the xy-plane.
    The index is built once and cannot be extended afterward.

    The index can be used to pre-select DXF entities from a certain area to reduce the
    search space for other selection tools of this module.

    **Functionality**

    - The index relies on the bounding boxes of DXF entities, and only the corner
      vertices of these bounding boxes are indexed except for POINT and LINE.
    - It can only find DXF entities that have at least one bounding box vertex located
      within the search area. Entities whose bounding boxes overlap the search area but
      have no vertices inside it will not be found (e.g., a circle whose center point
      is inside the search area but none of its bounding box vertices will not be
      included).
    - The detection behavior can be customized by overriding the :meth:`detection_points`
      method.

    **Recommendations**

    Since this index is intended to be used in conjunction with other selection tools
    within this module, it's recommended to maintain a bounding box cache to avoid
    the computational cost of recalculating them frequently. This class creates a new
    bounding box cache if none is specified. This cache can be accessed through the
    public attribute :attr:`cache`.

    """

    def __init__(
        self,
        entities: Iterable[DXFEntity],
        cache: bbox.Cache | None = None,
        max_node_size=5,  # Change only if you know what you do!
    ):
        class RTreeVtx(Vec2):  # super() doesn't work, so no __init__()
            __slots__ = ("uid",)

        def detection_vertex(location: Vec2, uid: int) -> RTreeVtx:
            vertex = RTreeVtx(location)
            vertex.uid = uid
            return vertex

        self.cache = cache or bbox.Cache()
        self._entities: dict[int, DXFEntity] = {}
        detection_vertices: list[RTreeVtx] = []
        for entity in entities:
            detection_points = self.detection_points(entity)
            if not detection_points:
                continue

            uid = id(entity)
            self._entities[uid] = entity
            detection_vertices.extend(
                detection_vertex(pnt, uid) for pnt in detection_points
            )
        self._search_tree = rtree.RTree(
            detection_vertices, max_node_size=max(5, int(max_node_size))
        )

    def detection_points(self, entity: DXFEntity) -> Sequence[Vec2]:
        """Returns the detection points for a given DXF entity.

        The detection points must be 2D points projected onto the xy-plane (ignore z-axis).
        This implementation returns the corner vertices of the entity bounding box.

        Override this method to return more sophisticated detection points
        (e.g., the vertices of LWPOLYLINE and POLYLINE or equally spaced raster points
        for block references).
        """
        dxftype = entity.dxftype()
        if dxftype == "POINT":
            return (Vec2(entity.dxf.location),)
        if dxftype == "LINE":
            return (Vec2(entity.dxf.start), Vec2(entity.dxf.end))

        box2d = BoundingBox2d(bbox.extents((entity,), fast=True, cache=self.cache))
        if box2d.has_data:
            return box2d.rect_vertices()
        return tuple()

    def detection_point_in_circle(
        self, center: UVec, radius: float
    ) -> Sequence[DXFEntity]:
        """Returns all DXF entities that have at least one detection point located
        around `center` with a max. distance of `radius`.
        """
        detection_vertices = self._search_tree.points_in_sphere(Vec2(center), radius)
        entities = self._entities
        return [entities[uid] for uid in set(v.uid for v in detection_vertices)]

    def detection_point_in_rect(self, p1: UVec, p2: UVec) -> Sequence[DXFEntity]:
        """Returns all DXF entities that have at least one detection point located
        inside or at the border of the rectangle defined by the two given corner points.
        """
        detection_vertices = self._search_tree.points_in_bbox(
            BoundingBox([Vec2(p1), Vec2(p2)])
        )
        entities = self._entities
        return [entities[uid] for uid in set(v.uid for v in detection_vertices)]
