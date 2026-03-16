import gc
import os
import pickle
import subprocess
import sys

from shapely.strtree import STRtree
from shapely.geometry import Point, Polygon

from shapely import strtree

from .conftest import requires_geos_342


@requires_geos_342
def test_query():
    points = [Point(i, i) for i in range(10)]
    tree = STRtree(points)
    results = tree.query(Point(2, 2).buffer(0.99))
    assert len(results) == 1
    results = tree.query(Point(2, 2).buffer(1.0))
    assert len(results) == 3


@requires_geos_342
def test_insert_empty_geometry():
    """
    Passing nothing but empty geometries results in an empty strtree.
    The query segfaults if the empty geometry was actually inserted.
    """
    empty = Polygon()
    geoms = [empty]
    tree = STRtree(geoms)
    assert tree._n_geoms == 0
    query = Polygon([(0, 0), (1, 1), (2, 0), (0, 0)])
    results = tree.query(query)
    assert len(results) == 0


@requires_geos_342
def test_query_empty_geometry():
    """
    Empty geometries should be filtered out.
    The query segfaults if the empty geometry was actually inserted.
    """
    empty = Polygon()
    point = Point(1, 0.5)
    geoms = [empty, point]
    tree = STRtree(geoms)
    assert tree._n_geoms == 1
    query = Polygon([(0, 0), (1, 1), (2, 0), (0, 0)])
    results = tree.query(query)
    assert len(results) == 1
    assert results[0] == point


@requires_geos_342
def test_references():
    """Don't crash due to dangling references"""
    empty = Polygon()
    point = Point(1, 0.5)
    geoms = [empty, point]
    tree = STRtree(geoms)
    assert tree._n_geoms == 1

    empty = None
    point = None
    gc.collect()

    query = Polygon([(0, 0), (1, 1), (2, 0), (0, 0)])
    results = tree.query(query)
    assert len(results) == 1
    assert results[0] == Point(1, 0.5)


@requires_geos_342
def test_safe_delete():
    tree = STRtree([])

    _lgeos = strtree.lgeos
    strtree.lgeos = None

    del tree

    strtree.lgeos = _lgeos


@requires_geos_342
def _test_pickle_persistence():
    """
    Don't crash trying to use unpickled GEOS handle.
    """
    tree = STRtree([Point(i, i).buffer(0.1) for i in range(3)])
    pickled_strtree = pickle.dumps(tree)
    unpickle_script_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "unpickle-strtree.py")
    proc = subprocess.Popen(
        [sys.executable, str(unpickle_script_file_path)],
        stdin=subprocess.PIPE,
    )
    proc.communicate(input=pickled_strtree)
    proc.wait()
    assert proc.returncode == 0
