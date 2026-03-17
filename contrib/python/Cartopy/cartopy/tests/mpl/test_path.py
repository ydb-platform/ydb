# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from matplotlib.path import Path
import pytest
import shapely.geometry as sgeom

import cartopy.mpl.patch as cpatch
import cartopy.mpl.path as cpath


@pytest.mark.parametrize('use_legacy_path_to_geos', [False, True])
class Test_path_to_shapely:
    def test_empty_polygon(self, use_legacy_path_to_geos):
        p = Path(
            [
                [0, 0], [0, 0], [0, 0], [0, 0],
                [1, 2], [1, 2], [1, 2], [1, 2],
                # The vertex for CLOSEPOLY should be ignored.
                [2, 3], [2, 3], [2, 3], [42, 42],
                # Very close points should be treated the same.
                [193.75, -14.166664123535156], [193.75, -14.166664123535158],
                [193.75, -14.166664123535156], [193.75, -14.166664123535156],
            ],
            codes=[1, 2, 2, 79] * 4)
        if use_legacy_path_to_geos:
            with pytest.warns(DeprecationWarning, match="path_to_geos is deprecated"):
                geoms = cpatch.path_to_geos(p)
            assert [type(geom) for geom in geoms] == [sgeom.Point] * 4
            assert len(geoms) == 4
        else:
            geoms = cpath.path_to_shapely(p)
            assert isinstance(geoms, sgeom.MultiPoint)
            assert len(geoms.geoms) == 4

    def test_non_polygon_loop(self, use_legacy_path_to_geos):
        p = Path([[0, 10], [170, 20], [-170, 30], [0, 10]],
                 codes=[1, 2, 2, 2])
        if use_legacy_path_to_geos:
            with pytest.warns(DeprecationWarning, match="path_to_geos is deprecated"):
                geoms = cpatch.path_to_geos(p)

            assert [type(geom) for geom in geoms] == [sgeom.MultiLineString]
            assert len(geoms) == 1
        else:
            geoms = cpath.path_to_shapely(p)
            assert isinstance(geoms, sgeom.LineString)

    def test_polygon_with_interior_and_singularity(self, use_legacy_path_to_geos):
        # A geometry with two interiors, one a single point.
        p = Path([[0, -90], [200, -40], [200, 40], [0, 40], [0, -90],
                  [126, 26], [126, 26], [126, 26], [126, 26], [126, 26],
                  [114, 5], [103, 8], [126, 12], [126, 0], [114, 5]],
                 codes=[1, 2, 2, 2, 79, 1, 2, 2, 2, 79, 1, 2, 2, 2, 79])
        if use_legacy_path_to_geos:
            with pytest.warns(DeprecationWarning, match="path_to_geos is deprecated"):
                geoms = cpatch.path_to_geos(p)

            assert [type(geom) for geom in geoms] == [sgeom.Polygon, sgeom.Point]
            assert len(geoms[0].interiors) == 1
        else:
            geoms = cpath.path_to_shapely(p)
            assert isinstance(geoms, sgeom.GeometryCollection)
            assert [type(geom) for geom in geoms.geoms] == [sgeom.Polygon, sgeom.Point]
            assert len(geoms.geoms[0].interiors) == 1

    def test_nested_polygons(self, use_legacy_path_to_geos):
        # A geometry with three nested squares.
        vertices = [[0, 0], [0, 10], [10, 10], [10, 0], [0, 0],
                    [2, 2], [2, 8], [8, 8], [8, 2], [2, 2],
                    [4, 4], [4, 6], [6, 6], [6, 4], [4, 4]]
        codes = [1, 2, 2, 2, 79, 1, 2, 2, 2, 79, 1, 2, 2, 2, 79]
        p = Path(vertices, codes=codes)

        # The first square makes the first geometry with the second square as
        # its interior.  The third square is its own geometry with no interior.
        if use_legacy_path_to_geos:
            with pytest.warns(DeprecationWarning, match="path_to_geos is deprecated"):
                geoms = cpatch.path_to_geos(p)

            assert len(geoms) == 2
            assert all(isinstance(geom, sgeom.Polygon) for geom in geoms)
            assert len(geoms[0].interiors) == 1
            assert len(geoms[1].interiors) == 0
        else:
            geoms = cpath.path_to_shapely(p)
            assert isinstance(geoms, sgeom.MultiPolygon)
            assert len(geoms.geoms) == 2
            assert len(geoms.geoms[0].interiors) == 1
            assert len(geoms.geoms[1].interiors) == 0
