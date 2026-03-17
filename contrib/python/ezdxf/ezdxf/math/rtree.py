# Copyright (c) 2022, Manfred Moitzi
# License: MIT License
# Immutable spatial search tree based on the SsTree implementation of the book
# "Advanced Algorithms and Data Structures"
# - SsTree JavaScript source code:
#   (c) 2019, Marcello La Rocca, released under the GNU Affero General Public License v3.0
#   https://github.com/mlarocca/AlgorithmsAndDataStructuresInAction/tree/master/JavaScript/src/ss_tree
# - Research paper of Antonin Guttman:
#   http://www-db.deis.unibo.it/courses/SI-LS/papers/Gut84.pdf
from __future__ import annotations
from operator import itemgetter
import statistics
from typing import Iterator, Callable, Sequence, Iterable, TypeVar, Generic
import abc
import math

from ezdxf.math import BoundingBox, Vec2, Vec3, spherical_envelope

__all__ = ["RTree"]

INF = float("inf")

T = TypeVar("T", Vec2, Vec3)


class Node(abc.ABC, Generic[T]):
    __slots__ = ("bbox",)

    def __init__(self, bbox: BoundingBox):
        self.bbox: BoundingBox = bbox

    @abc.abstractmethod
    def __len__(self) -> int: ...

    @abc.abstractmethod
    def __iter__(self) -> Iterator[T]: ...

    @abc.abstractmethod
    def contains(self, point: T) -> bool: ...

    @abc.abstractmethod
    def _nearest_neighbor(
        self, target: T, nn: T = None, nn_dist: float = INF
    ) -> tuple[T, float]: ...

    @abc.abstractmethod
    def points_in_sphere(self, center: T, radius: float) -> Iterator[T]: ...

    @abc.abstractmethod
    def points_in_bbox(self, bbox: BoundingBox) -> Iterator[T]: ...

    def nearest_neighbor(self, target: T) -> tuple[T, float]:
        return self._nearest_neighbor(target)


class LeafNode(Node[T]):
    __slots__ = ("points", "bbox")

    def __init__(self, points: list[T]):
        self.points = tuple(points)
        super().__init__(BoundingBox(self.points))

    def __len__(self):
        return len(self.points)

    def __iter__(self) -> Iterator[T]:
        return iter(self.points)

    def contains(self, point: T) -> bool:
        return any(point.isclose(p) for p in self.points)

    def _nearest_neighbor(
        self, target: T, nn: T = None, nn_dist: float = INF
    ) -> tuple[T, float]:
        distance, point = min((target.distance(p), p) for p in self.points)
        if distance < nn_dist:
            nn, nn_dist = point, distance
        return nn, nn_dist

    def points_in_sphere(self, center: T, radius: float) -> Iterator[T]:
        return (p for p in self.points if center.distance(p) <= radius)

    def points_in_bbox(self, bbox: BoundingBox) -> Iterator[T]:
        return (p for p in self.points if bbox.inside(p))


class InnerNode(Node[T]):
    __slots__ = ("children", "bbox")

    def __init__(self, children: Sequence[Node[T]]):
        super().__init__(BoundingBox())
        self.children = tuple(children)
        for child in self.children:
            # build union of all child bounding boxes
            self.bbox.extend([child.bbox.extmin, child.bbox.extmax])

    def __len__(self) -> int:
        return sum(len(c) for c in self.children)

    def __iter__(self) -> Iterator[T]:
        for child in self.children:
            yield from iter(child)

    def contains(self, point: T) -> bool:
        for child in self.children:
            if child.bbox.inside(point) and child.contains(point):
                return True
        return False

    def _nearest_neighbor(
        self, target: T, nn: T = None, nn_dist: float = INF
    ) -> tuple[T, float]:
        closest_child = find_closest_child(self.children, target)
        nn, nn_dist = closest_child._nearest_neighbor(target, nn, nn_dist)
        for child in self.children:
            if child is closest_child:
                continue
            # is target inside the child bounding box + nn_dist in all directions
            if grow_box(child.bbox, nn_dist).inside(target):
                point, distance = child._nearest_neighbor(target, nn, nn_dist)
                if distance < nn_dist:
                    nn = point
                    nn_dist = distance
        return nn, nn_dist

    def points_in_sphere(self, center: T, radius: float) -> Iterator[T]:
        for child in self.children:
            if is_sphere_intersecting_bbox(
                Vec3(center), radius, child.bbox.center, child.bbox.size
            ):
                yield from child.points_in_sphere(center, radius)

    def points_in_bbox(self, bbox: BoundingBox) -> Iterator[T]:
        for child in self.children:
            if bbox.has_overlap(child.bbox):
                yield from child.points_in_bbox(bbox)


class RTree(Generic[T]):
    """Immutable spatial search tree loosely based on `R-trees`_.

    The search tree is buildup once at initialization and immutable afterwards,
    because rebuilding the tree after inserting or deleting nodes is very costly
    and makes the implementation very complex.  
    
    Without the ability to alter the content the restrictions which forces the tree 
    balance at growing and shrinking of the original `R-trees`_, are ignored, like the 
    fixed minimum and maximum node size.

    This class uses internally only 3D bounding boxes, but also supports
    :class:`Vec2` as well as :class:`Vec3` objects as input data, but point
    types should not be mixed in a search tree.

    The point objects keep their type and identity and the returned points of
    queries can be compared by the ``is`` operator for identity to the input
    points.

    The implementation requires a maximum node size of at least 2 and
    does not support empty trees!

    Raises:
        ValueError: max. node size too small or no data given

    .. _R-trees: https://en.wikipedia.org/wiki/R-tree

    """

    __slots__ = ("_root",)

    def __init__(self, points: Iterable[T], max_node_size: int = 5):
        if max_node_size < 2:
            raise ValueError("max node size must be > 1")
        _points = list(points)
        if len(_points) == 0:
            raise ValueError("no points given")
        self._root = make_node(_points, max_node_size, box_split)

    def __len__(self):
        """Returns the count of points in the search tree."""
        return len(self._root)

    def __iter__(self) -> Iterator[T]:
        """Yields all points in the search tree."""
        yield from iter(self._root)

    def contains(self, point: T) -> bool:
        """Returns ``True`` if `point` exists, the comparison is done by the
        :meth:`isclose` method and not by the identity operator ``is``.
        """
        return self._root.contains(point)

    def nearest_neighbor(self, target: T) -> tuple[T, float]:
        """Returns the closest point to the `target` point and the distance
        between these points.
        """
        return self._root.nearest_neighbor(target)

    def points_in_sphere(self, center: T, radius: float) -> Iterator[T]:
        """Returns all points in the range of the given sphere including the
        points at the boundary.
        """
        return self._root.points_in_sphere(center, radius)

    def points_in_bbox(self, bbox: BoundingBox) -> Iterator[T]:
        """Returns all points in the range of the given bounding box including
        the points at the boundary.
        """
        return self._root.points_in_bbox(bbox)

    def avg_leaf_size(self, spread: float = 1.0) -> float:
        """Returns the average size of the leaf bounding boxes.
        The size of a leaf bounding box is the maximum size in all dimensions.
        Excludes outliers of sizes beyond mean + standard deviation * spread.
        Returns 0.0 if less than two points in tree.
        """
        sizes: list[float] = [
            max(leaf.bbox.size.xyz) for leaf in collect_leafs(self._root)
        ]
        return average_exclusive_outliers(sizes, spread)

    def avg_spherical_envelope_radius(self, spread: float = 1.0) -> float:
        """Returns the average radius of spherical envelopes of the leaf nodes.
        Excludes outliers with radius beyond mean + standard deviation * spread.
        Returns 0.0 if less than two points in tree.
        """
        radii: list[float] = [
            spherical_envelope(leaf.points)[1] for leaf in collect_leafs(self._root)
        ]
        return average_exclusive_outliers(radii, spread)

    def avg_nn_distance(self, spread: float = 1.0) -> float:
        """Returns the average of the nearest neighbor distances inside (!)
        leaf nodes. Excludes outliers with a distance beyond the overall
        mean + standard deviation * spread. Returns 0.0 if less than two points
        in tree.

        .. warning::

            This is a brute force check with O(n!) for each leaf node, where n
            is the point count of the leaf node.

        """
        distances: list[float] = []
        for leaf in collect_leafs(self._root):
            distances.extend(nearest_neighbor_distances(leaf.points))
        return average_exclusive_outliers(distances, spread)


def make_node(
    points: list[T],
    max_size: int,
    split_strategy: Callable[[list[T], int], Sequence[Node]],
) -> Node[T]:
    if len(points) > max_size:
        return InnerNode(split_strategy(points, max_size))
    else:
        return LeafNode(points)


def box_split(points: list[T], max_size: int) -> Sequence[Node[T]]:
    n = len(points)
    size: tuple[float, float, float] = BoundingBox(points).size.xyz
    dim = size.index(max(size))
    points.sort(key=itemgetter(dim))
    k = math.ceil(n / max_size)
    return tuple(
        make_node(points[i : i + k], max_size, box_split) for i in range(0, n, k)
    )


def is_sphere_intersecting_bbox(
    centroid: Vec3, radius: float, center: Vec3, size: Vec3
) -> bool:
    distance = centroid - center
    intersection_distance = size * 0.5 + Vec3(radius, radius, radius)
    # non-intersection is more often likely:
    if abs(distance.x) > intersection_distance.x:
        return False
    if abs(distance.y) > intersection_distance.y:
        return False
    if abs(distance.z) > intersection_distance.z:
        return False
    return True


def find_closest_child(children: Sequence[Node[T]], point: T) -> Node[T]:
    def distance(child: Node) -> float:
        return point.distance(child.bbox.center)

    assert len(children) > 0
    return min(children, key=distance)


def grow_box(box: BoundingBox, dist: float) -> BoundingBox:
    bbox = box.copy()
    bbox.grow(dist)
    return bbox


def average_exclusive_outliers(values: list[float], spread: float) -> float:
    if len(values) < 2:
        return 0.0
    stdev = statistics.stdev(values)
    mean = sum(values) / len(values)
    max_value = mean + stdev * spread
    values = [value for value in values if value <= max_value]
    if len(values):
        return sum(values) / len(values)
    return 0.0


def collect_leafs(node: Node[T]) -> Iterable[LeafNode[T]]:
    """Yields all leaf nodes below the given node."""
    if isinstance(node, LeafNode):
        yield node
    elif isinstance(node, InnerNode):
        for child in node.children:
            yield from collect_leafs(child)


def nearest_neighbor_distances(points: Sequence[T]) -> list[float]:
    """Brute force calculation of nearest neighbor distances with a
    complexity of O(n!).
    """
    return [
        min(point.distance(p) for p in points[index + 1 :])
        for index, point in enumerate(points[:-1])
    ]
