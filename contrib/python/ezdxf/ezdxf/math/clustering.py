#  Copyright (c) 2022, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterator, Iterable, Optional
import random
import statistics
import itertools
import operator
from collections import defaultdict
from functools import reduce

from ezdxf.math import AnyVec, Vec3, spherical_envelope
from ezdxf.math.rtree import RTree

__all__ = [
    "dbscan",
    "k_means",
    "average_cluster_radius",
    "average_intra_cluster_distance",
]


def dbscan(
    points: list[AnyVec],
    *,
    radius: float,
    min_points: int = 4,
    rtree: Optional[RTree] = None,
    max_node_size: int = 5,
) -> list[list[AnyVec]]:
    """DBSCAN clustering.

    https://en.wikipedia.org/wiki/DBSCAN

    Args:
        points: list of points to cluster
        radius: radius of the dense regions
        min_points: minimum number of points that needs to be within the
            `radius` for a point to be a core point (must be >= 2)
        rtree: optional :class:`~ezdxf.math.rtree.RTree`
        max_node_size: max node size for internally created RTree

    Returns:
        list of clusters, each cluster is a list of points

    """
    if min_points < 2:
        raise ValueError("min_points must be >= 2")
    if rtree is None:
        rtree = RTree(points, max_node_size)

    clusters: list[set[AnyVec]] = []
    point_set = set(points)
    while len(point_set):
        point = point_set.pop()
        todo = {point}
        cluster = {point}  # the cluster has only a single entry if noise
        clusters.append(cluster)
        while len(todo):
            chk_point = todo.pop()
            neighbors = set(rtree.points_in_sphere(chk_point, radius))
            if len(neighbors) < min_points:
                continue
            cluster.add(chk_point)
            point_set.discard(chk_point)
            todo |= neighbors.intersection(point_set)

    return [list(cluster) for cluster in clusters]


def k_means(
    points: list[AnyVec], k: int, max_iter: int = 10
) -> list[list[AnyVec]]:
    """K-means clustering.

    https://en.wikipedia.org/wiki/K-means_clustering

    Args:
        points: list of points to cluster
        k: number of clusters
        max_iter: max iterations

    Returns:
        list of clusters, each cluster is a list of points

    """

    def classify(centroids: Iterable[AnyVec]):
        new_clusters: dict[AnyVec, list[AnyVec]] = defaultdict(list)
        tree = RTree(centroids)
        for point in points:
            nn, _ = tree.nearest_neighbor(point)
            new_clusters[nn].append(point)
        return new_clusters

    def recenter() -> Iterator[AnyVec]:
        for cluster_points in clusters.values():
            yield Vec3.sum(cluster_points) / len(cluster_points)
        if len(clusters) < k:  # refill centroids if required
            yield from random.sample(points, k - len(clusters))

    def is_equal_clustering(old_clusters, new_clusters):
        def hash_list(lst):
            lst.sort()
            return reduce(operator.xor, map(hash, lst))

        h1 = sorted(map(hash_list, old_clusters.values()))
        h2 = sorted(map(hash_list, new_clusters.values()))
        return h1 == h2

    if not (1 < k < len(points)):
        raise ValueError(
            "invalid argument k: must be in range [2, len(points)-1]"
        )
    clusters: dict[AnyVec, list[AnyVec]] = classify(random.sample(points, k))
    for _ in range(max_iter):
        new_clusters = classify(recenter())
        if is_equal_clustering(clusters, new_clusters):
            break
        clusters = new_clusters
    return list(clusters.values())


def average_intra_cluster_distance(clusters: list[list[AnyVec]]) -> float:
    """Returns the average point-to-point intra cluster distance."""

    return statistics.mean(
        [
            p.distance(q)
            for cluster in clusters
            for (p, q) in itertools.combinations(cluster, 2)
        ]
    )


def average_cluster_radius(clusters: list[list[AnyVec]]) -> float:
    """Returns the average cluster radius."""

    return statistics.mean(
        [spherical_envelope(cluster)[1] for cluster in clusters]
    )
