# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import itertools
import time

import numpy as np
import pytest
import shapely
import shapely.geometry as sgeom

import cartopy.crs as ccrs


class TestLineString:
    def test_out_of_bounds(self):
        # Check that a line that is completely out of the map boundary produces
        # a valid LineString
        projection = ccrs.TransverseMercator(central_longitude=0, approx=True)

        # For both start & end, define a point that results in well-defined
        # projection coordinates and one that results in NaN.
        start_points = [(86, 0), (130, 0)]
        end_points = [(88, 0), (120, 0)]

        # Try all four combinations of valid/NaN vs valid/NaN.
        for start, end in itertools.product(start_points, end_points):
            line_string = sgeom.LineString([start, end])
            multi_line_string = projection.project_geometry(line_string)
            if start[0] == 130 and end[0] == 120:
                expected = 0
            else:
                expected = 1
            assert len(multi_line_string.geoms) == expected, \
                f'Unexpected line when working from {start} to {end}'

    def test_simple_fragment_count(self):
        projection = ccrs.PlateCarree()

        tests = [
            ([(150, 0), (-150, 0)], 2),
            ([(10, 0), (90, 0), (180, 0), (-90, 0), (-10, 0)], 2),
            ([(-10, 0), (10, 0)], 1),
            ([(-45, 0), (45, 30)], 1),
        ]

        for coords, pieces in tests:
            line_string = sgeom.LineString(coords)
            multi_line_string = projection.project_geometry(line_string)
            # from cartopy.tests.mpl import show
            # show(projection, multi_line_string)
            assert len(multi_line_string.geoms) == pieces

    def test_split(self):
        projection = ccrs.Robinson(170.5)
        line_string = sgeom.LineString([(-10, 30), (10, 60)])
        multi_line_string = projection.project_geometry(line_string)
        # from cartopy.tests.mpl import show
        # show(projection, multi_line_string)
        assert len(multi_line_string.geoms) == 2

    def test_out_of_domain_efficiency(self):
        # Check we're efficiently dealing with lines that project
        # outside the map domain.
        # Because the south pole projects to an *enormous* circle
        # (radius ~ 1e23) this will take a *long* time to project if the
        # within-domain exactness criteria are used.
        line_string = sgeom.LineString([(0, -90), (2, -90)])
        tgt_proj = ccrs.NorthPolarStereo()
        src_proj = ccrs.PlateCarree()
        cutoff_time = time.time() + 1
        tgt_proj.project_geometry(line_string, src_proj)
        assert time.time() < cutoff_time, 'Projection took too long'

    @pytest.mark.skipif(shapely.__version__ < "2",
                        reason="Shapely <2 has an incorrect geom_type ")
    def test_multi_linestring_return_type(self):
        # Check that the return type of project_geometry is a MultiLineString
        # and not an empty list
        multi_line_string = ccrs.Mercator().project_geometry(
            sgeom.MultiLineString(), ccrs.PlateCarree())
        assert isinstance(multi_line_string, sgeom.MultiLineString)


class FakeProjection(ccrs.PlateCarree):
    def __init__(self, left_offset=0, right_offset=0):
        self.left_offset = left_offset
        self.right_offset = right_offset

        self._half_width = 180
        self._half_height = 90
        ccrs.PlateCarree.__init__(self)

    @property
    def boundary(self):
        # XXX Should this be a LinearRing?
        w, h = self._half_width, self._half_height
        return sgeom.LineString([(-w + self.left_offset, -h),
                                 (-w + self.left_offset, h),
                                 (w - self.right_offset, h),
                                 (w - self.right_offset, -h),
                                 (-w + self.left_offset, -h)])


class TestBisect:
    # A bunch of tests to check the bisection algorithm is robust for a
    # variety of simple and/or pathological cases.

    def test_repeated_point(self):
        projection = FakeProjection()
        line_string = sgeom.LineString([(10, 0), (10, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string.geoms[0].coords) == 2

    def test_interior_repeated_point(self):
        projection = FakeProjection()
        line_string = sgeom.LineString([(0, 0), (10, 0), (10, 0), (20, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string.geoms[0].coords) == 4

    def test_circular_repeated_point(self):
        projection = FakeProjection()
        line_string = sgeom.LineString([(0, 0), (360, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string.geoms[0].coords) == 2

    def test_short(self):
        projection = FakeProjection()
        line_string = sgeom.LineString([(0, 0), (1e-12, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string.geoms[0].coords) == 2

    def test_empty(self):
        projection = FakeProjection(right_offset=10)
        line_string = sgeom.LineString([(175, 0), (175, 10)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 0

    def test_simple_run_in(self):
        projection = FakeProjection(right_offset=10)
        line_string = sgeom.LineString([(160, 0), (175, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string.geoms[0].coords) == 2

    def test_simple_wrap(self):
        projection = FakeProjection()
        line_string = sgeom.LineString([(160, 0), (-160, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 2
        assert len(multi_line_string.geoms[0].coords) == 2
        assert len(multi_line_string.geoms[1].coords) == 2

    def test_simple_run_out(self):
        projection = FakeProjection(left_offset=10)
        line_string = sgeom.LineString([(-175, 0), (-160, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string.geoms[0].coords) == 2

    def test_point_on_boundary(self):
        projection = FakeProjection()
        line_string = sgeom.LineString([(180, 0), (-160, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string.geoms[0].coords) == 2

        # Add a small offset to the left-hand boundary to make things
        # even more pathological.
        projection = FakeProjection(left_offset=5)
        line_string = sgeom.LineString([(180, 0), (-160, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string.geoms[0].coords) == 2

    def test_nan_start(self):
        projection = ccrs.TransverseMercator(central_longitude=-90,
                                             approx=False)
        line_string = sgeom.LineString([(10, 50), (-10, 30)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        for line_string in multi_line_string.geoms:
            for coord in line_string.coords:
                assert not any(np.isnan(coord)), \
                    'Unexpected NaN in projected coords.'

    def test_nan_end(self):
        projection = ccrs.TransverseMercator(central_longitude=-90,
                                             approx=False)
        line_string = sgeom.LineString([(-10, 30), (10, 50)])
        multi_line_string = projection.project_geometry(line_string)
        # from cartopy.tests.mpl import show
        # show(projection, multi_line_string)
        assert len(multi_line_string.geoms) == 1
        for line_string in multi_line_string.geoms:
            for coord in line_string.coords:
                assert not any(np.isnan(coord)), \
                    'Unexpected NaN in projected coords.'

    def test_nan_rectangular(self):
        # Make sure rectangular projections can handle invalid geometries
        projection = ccrs.Robinson()
        line_string = sgeom.LineString([(0, 0), (1, 1), (np.nan, np.nan),
                                        (2, 2), (3, 3)])
        multi_line_string = projection.project_geometry(line_string,
                                                        ccrs.PlateCarree())
        assert len(multi_line_string.geoms) == 2


class TestMisc:
    def test_misc(self):
        projection = ccrs.TransverseMercator(central_longitude=-90,
                                             approx=False)
        line_string = sgeom.LineString([(10, 50), (-10, 30)])
        multi_line_string = projection.project_geometry(line_string)
        # from cartopy.tests.mpl import show
        # show(projection, multi_line_string)
        for line_string in multi_line_string.geoms:
            for coord in line_string.coords:
                assert not any(np.isnan(coord)), \
                    'Unexpected NaN in projected coords.'

    def test_something(self):
        projection = ccrs.RotatedPole(pole_longitude=177.5,
                                      pole_latitude=37.5)
        line_string = sgeom.LineString([(0, 0), (1e-14, 0)])
        multi_line_string = projection.project_geometry(line_string)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string.geoms[0].coords) == 2

    def test_global_boundary(self):
        linear_ring = sgeom.LineString([(-180, -180), (-180, 180),
                                        (180, 180), (180, -180)])
        pc = ccrs.PlateCarree()
        merc = ccrs.Mercator()
        multi_line_string = pc.project_geometry(linear_ring, merc)
        assert len(multi_line_string.geoms) > 0

        # check the identity transform
        multi_line_string = merc.project_geometry(linear_ring, merc)
        assert len(multi_line_string.geoms) > 0


class TestSymmetry:
    @pytest.mark.xfail
    def test_curve(self):
        # Obtain a simple, curved path.
        projection = ccrs.PlateCarree()
        coords = [(-0.08, 51.53), (132.00, 43.17)]  # London to Vladivostock
        line_string = sgeom.LineString(coords)
        multi_line_string = projection.project_geometry(line_string)

        # Compute the reverse path.
        line_string = sgeom.LineString(coords[::-1])
        multi_line_string2 = projection.project_geometry(line_string)

        # Make sure that they generated the same points.
        # (Although obviously they will be in the opposite order!)
        assert len(multi_line_string.geoms) == 1
        assert len(multi_line_string2.geoms) == 1
        coords = multi_line_string.geoms[0].coords
        coords2 = multi_line_string2.geoms[0].coords
        np.testing.assert_allclose(coords, coords2[::-1],
                                   err_msg='Asymmetric curve generation')
