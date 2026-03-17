# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import numpy as np
import pytest
import shapely.geometry as sgeom
import shapely.wkt

import cartopy.crs as ccrs


class TestBoundary:
    def test_no_polygon_boundary_reversal(self):
        # Check that polygons preserve their clockwise or counter-clockwise
        # ordering when they are attached to the boundary.
        # Failure to do so will result in invalid polygons (their boundaries
        # cross-over).
        polygon = sgeom.Polygon([(-10, 30), (10, 60), (10, 50)])
        projection = ccrs.Robinson(170.5)
        multi_polygon = projection.project_geometry(polygon)
        for polygon in multi_polygon.geoms:
            assert polygon.is_valid

    def test_polygon_boundary_attachment(self):
        # Check the polygon is attached to the boundary even when no
        # intermediate point for one of the crossing segments would normally
        # exist.
        polygon = sgeom.Polygon([(-10, 30), (10, 60), (10, 50)])
        projection = ccrs.Robinson(170.6)
        # This will raise an exception if the polygon/boundary intersection
        # fails.
        projection.project_geometry(polygon)

    def test_out_of_bounds(self):
        # Check that a polygon that is completely out of the map boundary
        # doesn't produce an empty result.
        projection = ccrs.TransverseMercator(central_longitude=0, approx=True)

        polys = [
            # All valid
            ([(86, -1), (86, 1), (88, 1), (88, -1)], 1),
            # One out of backwards projection range
            ([(86, -1), (86, 1), (130, 1), (88, -1)], 1),
            # An out of backwards projection range segment
            ([(86, -1), (86, 1), (130, 1), (130, -1)], 1),
            # All out of backwards projection range
            ([(120, -1), (120, 1), (130, 1), (130, -1)], 0),
        ]

        # Try all four combinations of valid/NaN vs valid/NaN.
        for coords, expected_polys in polys:
            polygon = sgeom.Polygon(coords)
            multi_polygon = projection.project_geometry(polygon)
            assert len(multi_polygon.geoms) == expected_polys


class TestMisc:
    def test_misc(self):
        projection = ccrs.TransverseMercator(central_longitude=-90,
                                             approx=False)
        polygon = sgeom.Polygon([(-10, 30), (10, 60), (10, 50)])
        projection.project_geometry(polygon)

    def test_small(self):
        projection = ccrs.Mercator()
        polygon = sgeom.Polygon([
            (-179.7933201090486079, -16.0208822567412312),
            (-180.0000000000000000, -16.0671326636424396),
            (-179.9173693847652942, -16.5017831356493616),
        ])
        multi_polygon = projection.project_geometry(polygon)
        assert len(multi_polygon.geoms) == 1
        assert len(multi_polygon.geoms[0].exterior.coords) == 4

    def test_former_infloop_case(self):
        # test a polygon which used to get stuck in an infinite loop
        # see https://github.com/SciTools/cartopy/issues/60
        coords = [(260.625, 68.90383337092122), (360.0, 79.8556091996901),
                  (360.0, 77.76848175458498), (0.0, 88.79068047337279),
                  (210.0, 90.0), (135.0, 88.79068047337279),
                  (260.625, 68.90383337092122)]
        geom = sgeom.Polygon(coords)

        target_projection = ccrs.PlateCarree()
        source_crs = ccrs.Geodetic()

        multi_polygon = target_projection.project_geometry(geom, source_crs)
        # check the result is non-empty
        assert not multi_polygon.is_empty

    def test_project_previous_infinite_loop(self):
        mstring1 = shapely.wkt.loads(
            'MULTILINESTRING ('
            '(-179.9999990464349651 -80.2000000000000171, '
            '-179.5000000001111005 -80.2000000000000171, '
            '-179.5000000001111005 -79.9000000000000199, '
            '-179.9999995232739138 -79.9499999523163041, '
            '-179.8000000001110550 -80.0000000000000000, '
            '-179.8000000001110550 -80.0999999999999943, '
            '-179.9999999047436177 -80.0999999999999943), '
            '(179.9999995231628702 -79.9499999523163041, '
            '179.5000000000000000 -79.9000000000000199, '
            '179.5000000000000000 -80.0000000000000000, '
            '179.9999995231628702 -80.0499999523162842, '
            '179.5000000000000000 -80.0999999999999943, '
            '179.5000000000000000 -80.2000000000000171, '
            '179.9999990463256836 -80.2000000000000171))')
        mstring2 = shapely.wkt.loads(
            'MULTILINESTRING ('
            '(179.9999996185302678 -79.9999999904632659, '
            '179.5999999999999943 -79.9899999999999949, '
            '179.5999999999999943 -79.9399999999999977, '
            '179.9999996185302678 -79.9599999809265114), '
            '(-179.9999999047436177 -79.9600000000000080, '
            '-179.9000000001110777 -79.9600000000000080, '
            '-179.9000000001110777 -80.0000000000000000, '
            '-179.9999999047436177 -80.0000000000000000))')
        multi_line_strings = [mstring1, mstring2]

        src = ccrs.PlateCarree()
        src._attach_lines_to_boundary(multi_line_strings, True)

    @pytest.mark.parametrize('proj',
                             [ccrs.InterruptedGoodeHomolosine, ccrs.Mollweide])
    def test_infinite_loop_bounds(self, proj):
        # test a polygon which used to get stuck in an infinite loop but is now
        # erroneously clipped.
        # see https://github.com/SciTools/cartopy/issues/1131

        # These names are for IGH; effectively the same for Mollweide.
        bottom = [0., 70.]
        right = [0., 90.]
        top = [-180., 90.]
        left = [-180., 70.]
        verts = np.array([
            bottom,
            right,
            top,
            left,
            bottom,
        ])
        bad_path = sgeom.Polygon(verts)

        target = proj()
        source = ccrs.PlateCarree()

        projected = target.project_geometry(bad_path, source)

        # When transforming segments was broken, the resulting path did not
        # close, and either filled most of the domain, or a smaller portion
        # than it should. Check that the bounds match the individual points at
        # the expected edges.
        projected_left = target.transform_point(left[0], left[1], source)
        assert projected.bounds[0] == pytest.approx(projected_left[0],
                                                    rel=target.threshold)
        projected_bottom = target.transform_point(bottom[0], bottom[1], source)
        assert projected.bounds[1] == pytest.approx(projected_bottom[1],
                                                    rel=target.threshold)
        projected_right = target.transform_point(right[0], right[1], source)
        assert projected.bounds[2] == pytest.approx(projected_right[0],
                                                    rel=target.threshold,
                                                    abs=1e-8)
        projected_top = target.transform_point(top[0], top[1], source)
        assert projected.bounds[3] == pytest.approx(projected_top[1],
                                                    rel=target.threshold)

    def test_3pt_poly(self):
        projection = ccrs.OSGB(approx=True)
        polygon = sgeom.Polygon([(-1000, -1000),
                                 (-1000, 200000),
                                 (200000, -1000)])
        multi_polygon = projection.project_geometry(polygon,
                                                    ccrs.OSGB(approx=True))
        assert len(multi_polygon.geoms) == 1
        assert len(multi_polygon.geoms[0].exterior.coords) == 4

    def test_self_intersecting_1(self):
        # Geometry comes from a matplotlib contourf (see #537)
        wkt = ('POLYGON ((366.22000122 -9.71489298, '
               '366.73212393 -9.679999349999999, '
               '366.77412634 -8.767753000000001, '
               '366.17762962 -9.679999349999999, '
               '366.22000122 -9.71489298), '
               '(366.22000122 -9.692636309999999, '
               '366.32998657 -9.603356099999999, '
               '366.74765799 -9.019999500000001, '
               '366.5094086 -9.63175386, '
               '366.22000122 -9.692636309999999))')
        geom = shapely.wkt.loads(wkt)
        source, target = ccrs.RotatedPole(198.0, 39.25), ccrs.EuroPP()
        projected = target.project_geometry(geom, source)
        # Before handling self intersecting interiors, the area would be
        # approximately 13262233761329.
        area = projected.area
        assert 2.2e9 < area < 2.3e9, \
            f'Got area {area}, expecting ~2.2e9'

    def test_self_intersecting_2(self):
        # Geometry comes from a matplotlib contourf (see #509)
        wkt = ('POLYGON ((343 20, 345 23, 342 25, 343 22, '
               '340 25, 341 25, 340 25, 343 20), (343 21, '
               '343 22, 344 23, 343 21))')
        geom = shapely.wkt.loads(wkt)
        source = target = ccrs.RotatedPole(193.0, 41.0)
        projected = target.project_geometry(geom, source)
        # Before handling self intersecting interiors, the area would be
        # approximately 64808.
        assert 7.9 < projected.area < 8.1

    def test_tiny_point_between_boundary_points(self):
        # Geometry comes from #259.
        target = ccrs.Orthographic(0, -75)
        source = ccrs.PlateCarree()
        wkt = 'POLYGON ((132 -40, 133 -6, 125.3 1, 115 -6, 132 -40))'
        geom = shapely.wkt.loads(wkt)

        target = ccrs.Orthographic(central_latitude=90., central_longitude=0)
        source = ccrs.PlateCarree()
        projected = target.project_geometry(geom, source)
        area = projected.area
        # Before fixing, this geometry used to fill the whole disk. Approx
        # 1.2e14.
        assert 81330 < area < 81340, \
            f'Got area {area}, expecting ~81336'

    def test_same_points_on_boundary_1(self):
        source = ccrs.PlateCarree()
        target = ccrs.PlateCarree(central_longitude=180)

        geom = sgeom.Polygon([(-20, -20), (20, -20), (20, 20), (-20, 20)],
                             [[(-10, 0), (0, 20), (10, 0), (0, -20)]])
        projected = target.project_geometry(geom, source)

        assert abs(1200 - projected.area) < 1e-5

    def test_same_points_on_boundary_2(self):
        source = ccrs.PlateCarree()
        target = ccrs.PlateCarree(central_longitude=180)

        geom = sgeom.Polygon([(-20, -20), (20, -20), (20, 20), (-20, 20)],
                             [[(0, 0), (-10, 10), (0, 20), (10, 10)],
                              [(0, 0), (10, -10), (0, -20), (-10, -10)]])
        projected = target.project_geometry(geom, source)

        assert abs(1200 - projected.area) < 1e-5

    def test_attach_short_loop(self):
        # Geometry comes from a matplotlib contourf.
        mstring = shapely.wkt.loads(
            'MULTILINESTRING ('
            '(-179.9999982118607 71.87500000000001,'
            '-179.0625 71.87500000000001,'
            '-179.9999982118607 71.87500000000001))')
        multi_line_strings = [mstring]

        src = ccrs.PlateCarree()
        polygons = src._attach_lines_to_boundary(multi_line_strings, True)
        # Before fixing, this would contain a geometry which would
        # cause a segmentation fault.
        assert polygons == []

    def test_project_degenerate_poly(self):
        # Tests for the same bug as test_attach_short_loop.
        # This test calls only the public API, but will cause a
        # segmentation fault when it fails.
        # Geometry comes from a matplotlib contourf.
        polygon = shapely.wkt.loads(
            'POLYGON (('
            '178.9687499944748 70.625, '
            '179.0625 71.875, '
            '180.9375 71.875, '
            '179.0625 71.875, '
            '177.1875 71.875, '
            '178.9687499944748 70.625))')

        source = ccrs.PlateCarree()
        target = ccrs.PlateCarree()
        # Before fixing, this would cause a segmentation fault.
        polygons = target.project_geometry(polygon, source)
        assert isinstance(polygons, sgeom.MultiPolygon)


class TestQuality:
    def setup_class(self):
        projection = ccrs.RotatedPole(pole_longitude=177.5,
                                      pole_latitude=37.5)
        polygon = sgeom.Polygon([
            (177.5, -57.38460319),
            (180.1, -57.445077),
            (175.0, -57.19913331),
        ])
        self.multi_polygon = projection.project_geometry(polygon)
        # from cartopy.tests.mpl import show
        # show(projection, self.multi_polygon)

    def test_split(self):
        # Start simple ... there should be two projected polygons.
        assert len(self.multi_polygon.geoms) == 2

    def test_repeats(self):
        # Make sure we don't have repeated points at the boundary, because
        # they mess up the linear extrapolation to the boundary.

        # Make sure there aren't any repeated points.
        xy = np.array(self.multi_polygon.geoms[0].exterior.coords)
        same = (xy[1:] == xy[:-1]).all(axis=1)
        assert not any(same), 'Repeated points in projected geometry.'

    def test_symmetry(self):
        # Make sure the number of points added on the way towards the
        # boundary is similar to the number of points added on the way away
        # from the boundary.

        # Identify all the contiguous sets of non-boundary points.
        xy = np.array(self.multi_polygon.geoms[0].exterior.coords)
        boundary = np.logical_or(xy[:, 1] == 90, xy[:, 1] == -90)
        regions = (boundary[1:] != boundary[:-1]).cumsum()
        regions = np.insert(regions, 0, 0)

        # For each region, check if the number of increasing steps is roughly
        # equal to the number of decreasing steps.
        for i in range(int(boundary[0]), regions.max(), 2):
            indices = np.where(regions == i)
            x = xy[indices, 0]
            delta = np.diff(x)
            num_incr = np.count_nonzero(delta > 0)
            num_decr = np.count_nonzero(delta < 0)
            assert abs(num_incr - num_decr) < 3, 'Too much asymmetry.'


class PolygonTests:
    def _assert_bounds(self, bounds, x1, y1, x2, y2, delta=1):
        assert abs(bounds[0] - x1) < delta
        assert abs(bounds[1] - y1) < delta
        assert abs(bounds[2] - x2) < delta
        assert abs(bounds[3] - y2) < delta


class TestWrap(PolygonTests):
    # Test that Plate Carree projection "does the right thing"(tm) with
    # source data that extends outside the [-180, 180] range.
    def test_plate_carree_no_wrap(self):
        proj = ccrs.PlateCarree()
        poly = sgeom.box(0, 0, 10, 10)
        multi_polygon = proj.project_geometry(poly, proj)
        # Check the structure
        assert len(multi_polygon.geoms) == 1
        # Check the rough shape
        polygon = multi_polygon.geoms[0]
        self._assert_bounds(polygon.bounds, 0, 0, 10, 10)

    def test_plate_carree_partial_wrap(self):
        proj = ccrs.PlateCarree()
        poly = sgeom.box(170, 0, 190, 10)
        multi_polygon = proj.project_geometry(poly, proj)
        # Check the structure
        assert len(multi_polygon.geoms) == 2
        # Check the rough shape
        poly1, poly2 = multi_polygon.geoms
        # The order of these polygons is not guaranteed, so figure out
        # which is appropriate
        if 170.0 not in poly1.bounds:
            poly1, poly2 = poly2, poly1
        self._assert_bounds(poly1.bounds, 170, 0, 180, 10)
        self._assert_bounds(poly2.bounds, -180, 0, -170, 10)

    def test_plate_carree_wrap(self):
        proj = ccrs.PlateCarree()
        poly = sgeom.box(200, 0, 220, 10)
        multi_polygon = proj.project_geometry(poly, proj)
        # Check the structure
        assert len(multi_polygon.geoms) == 1
        # Check the rough shape
        polygon = multi_polygon.geoms[0]
        self._assert_bounds(polygon.bounds, -160, 0, -140, 10)


def ring(minx, miny, maxx, maxy, ccw):
    box = sgeom.box(minx, miny, maxx, maxy, ccw)
    return np.array(box.exterior.coords)


class TestHoles(PolygonTests):
    def test_simple(self):
        proj = ccrs.PlateCarree()
        poly = sgeom.Polygon(ring(-40, -40, 40, 40, True),
                             [ring(-20, -20, 20, 20, False)])
        multi_polygon = proj.project_geometry(poly)
        # Check the structure
        assert len(multi_polygon.geoms) == 1
        assert len(multi_polygon.geoms[0].interiors) == 1
        # Check the rough shape
        polygon = multi_polygon.geoms[0]
        self._assert_bounds(polygon.bounds, -40, -47, 40, 47)
        self._assert_bounds(polygon.interiors[0].bounds, -20, -21, 20, 21)

    def test_wrapped_poly_simple_hole(self):
        proj = ccrs.PlateCarree(-150)
        poly = sgeom.Polygon(ring(-40, -40, 40, 40, True),
                             [ring(-20, -20, 20, 20, False)])
        multi_polygon = proj.project_geometry(poly)
        # Check the structure
        assert len(multi_polygon.geoms) == 2

        poly1, poly2 = multi_polygon.geoms
        # The order of these polygons is not guaranteed, so figure out
        # which is appropriate
        if not len(poly1.interiors) == 1:
            poly1, poly2 = poly2, poly1

        assert len(poly1.interiors) == 1
        assert len(poly2.interiors) == 0
        # Check the rough shape
        self._assert_bounds(poly1.bounds, 110, -47, 180, 47)
        self._assert_bounds(poly1.interiors[0].bounds, 130, -21, 170, 21)
        self._assert_bounds(poly2.bounds, -180, -43, -170, 43)

    def test_wrapped_poly_wrapped_hole(self):
        proj = ccrs.PlateCarree(-180)
        poly = sgeom.Polygon(ring(-40, -40, 40, 40, True),
                             [ring(-20, -20, 20, 20, False)])
        multi_polygon = proj.project_geometry(poly)
        # Check the structure
        assert len(multi_polygon.geoms) == 2
        assert len(multi_polygon.geoms[0].interiors) == 0
        assert len(multi_polygon.geoms[1].interiors) == 0
        # Check the rough shape
        polygon = multi_polygon.geoms[0]
        self._assert_bounds(polygon.bounds, 140, -47, 180, 47)
        polygon = multi_polygon.geoms[1]
        self._assert_bounds(polygon.bounds, -180, -47, -140, 47)

    def test_inverted_poly_simple_hole(self):
        proj = ccrs.NorthPolarStereo()
        poly = sgeom.Polygon([(0, 0), (-90, 0), (-180, 0), (-270, 0)],
                             [[(0, -30), (90, -30), (180, -30), (270, -30)]])
        multi_polygon = proj.project_geometry(poly)
        # Check the structure
        assert len(multi_polygon.geoms) == 1
        assert len(multi_polygon.geoms[0].interiors) == 1
        # Check the rough shape
        polygon = multi_polygon.geoms[0]
        self._assert_bounds(polygon.bounds, -2.4e7, -2.4e7, 2.4e7, 2.4e7, 1e6)
        self._assert_bounds(polygon.interiors[0].bounds,
                            - 1.2e7, -1.2e7, 1.2e7, 1.2e7, 1e6)

    def test_inverted_poly_multi_hole(self):
        # Adapted from 1149
        proj = ccrs.LambertAzimuthalEqualArea(
            central_latitude=45, central_longitude=-100)
        poly = sgeom.Polygon([(-180, -80), (180, -80), (180, 90), (-180, 90)],
                             [[(-50, -50), (-50, 0), (0, 0), (0, -50)]])
        multi_polygon = proj.project_geometry(poly)
        # Should project to single polygon with multiple holes
        assert len(multi_polygon.geoms) == 1
        assert len(multi_polygon.geoms[0].interiors) >= 2

    def test_inverted_poly_merged_holes(self):
        proj = ccrs.LambertAzimuthalEqualArea(central_latitude=-90)
        pc = ccrs.PlateCarree()
        poly = sgeom.Polygon([(-180, -80), (180, -80), (180, 90), (-180, 90)],
                             [[(-50, 60), (-50, 80), (0, 80), (0, 60)],
                              [(-50, 81), (-50, 85), (0, 85), (0, 81)]])
        # Smoke test that nearby holes do not cause side location conflict
        proj.project_geometry(poly, pc)

    def test_inverted_poly_clipped_hole(self):
        proj = ccrs.NorthPolarStereo()
        poly = sgeom.Polygon([(0, 0), (-90, 0), (-180, 0), (-270, 0)],
                             [[(-135, -60), (-45, -60),
                               (45, -60), (135, -60)]])
        multi_polygon = proj.project_geometry(poly)
        # Check the structure
        assert len(multi_polygon.geoms) == 1
        assert len(multi_polygon.geoms[0].interiors) == 1
        # Check the rough shape
        polygon = multi_polygon.geoms[0]
        self._assert_bounds(polygon.bounds, -5.0e7, -5.0e7, 5.0e7, 5.0e7, 1e6)
        self._assert_bounds(polygon.interiors[0].bounds,
                            - 1.2e7, -1.2e7, 1.2e7, 1.2e7, 1e6)
        assert abs(polygon.area - 7.30e15) < 1e13

    def test_inverted_poly_removed_hole(self):
        proj = ccrs.NorthPolarStereo(globe=ccrs.Globe(ellipse='WGS84'))
        poly = sgeom.Polygon([(0, 0), (-90, 0), (-180, 0), (-270, 0)],
                             [[(-135, -75), (-45, -75),
                               (45, -75), (135, -75)]])
        multi_polygon = proj.project_geometry(poly)
        # Check the structure
        assert len(multi_polygon.geoms) == 1
        assert len(multi_polygon.geoms[0].interiors) == 1
        # Check the rough shape
        polygon = multi_polygon.geoms[0]
        self._assert_bounds(polygon.bounds, -5.0e7, -5.0e7, 5.0e7, 5.0e7, 1e6)
        self._assert_bounds(polygon.interiors[0].bounds,
                            - 1.2e7, -1.2e7, 1.2e7, 1.2e7, 1e6)
        assert abs(polygon.area - 7.34e15) < 1e13

    def test_multiple_interiors(self):
        exterior = ring(0, 0, 12, 12, True)
        interiors = [ring(1, 1, 2, 2, False), ring(1, 8, 2, 9, False)]

        poly = sgeom.Polygon(exterior, interiors)

        target = ccrs.PlateCarree()
        source = ccrs.Geodetic()

        assert len(list(target.project_geometry(poly, source).geoms)) == 1
