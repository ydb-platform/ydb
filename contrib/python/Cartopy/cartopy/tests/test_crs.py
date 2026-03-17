# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import copy
from io import BytesIO
import os
from pathlib import Path
import pickle
import warnings

import numpy as np
from numpy.testing import assert_almost_equal, assert_array_equal
from numpy.testing import assert_array_almost_equal as assert_arr_almost_eq
import pyproj
import pytest
import shapely.geometry as sgeom

import cartopy.crs as ccrs


class TestCRS:
    def test_hash(self):
        stereo = ccrs.Stereographic(90)
        north = ccrs.NorthPolarStereo()
        assert stereo == north
        assert not stereo != north
        assert hash(stereo) == hash(north)

        assert ccrs.Geodetic() == ccrs.Geodetic()

    @pytest.mark.parametrize('approx', [True, False])
    def test_osni(self, approx):
        osni = ccrs.OSNI(approx=approx)
        ll = ccrs.Geodetic()

        # results obtained by nearby.org.uk.
        lon, lat = np.array([-5.54159863617957, 54.5622169298669],
                            dtype=np.double)
        east, north = np.array([359000, 371000], dtype=np.double)

        assert_arr_almost_eq(osni.transform_point(lon, lat, ll),
                             np.array([east, north]),
                             -1)
        assert_arr_almost_eq(ll.transform_point(east, north, osni),
                             np.array([lon, lat]),
                             3)

    def _check_osgb(self, osgb):
        precision = 1

        if os.environ.get('PROJ_NETWORK') != 'ON':
            grid_name = 'uk_os_OSTN15_NTv2_OSGBtoETRS.tif'
            available = (
                Path(pyproj.datadir.get_data_dir(), grid_name).exists() or
                Path(pyproj.datadir.get_user_data_dir(), grid_name).exists()
            )
            if not available:
                import warnings
                warnings.warn(f'{grid_name} is unavailable; '
                              'testing OSGB at reduced precision')
                precision = -1

        ll = ccrs.Geodetic()

        # results obtained by streetmap.co.uk.
        lon, lat = np.array([-3.478831, 50.462023], dtype=np.double)
        east, north = np.array([295132.1, 63512.6], dtype=np.double)

        # note the handling of precision here...
        assert_almost_equal(osgb.transform_point(lon, lat, ll), [east, north],
                            decimal=precision)
        assert_almost_equal(ll.transform_point(east, north, osgb), [lon, lat],
                            decimal=2)

        r_lon, r_lat = ll.transform_point(east, north, osgb)
        r_inverted = np.array(osgb.transform_point(r_lon, r_lat, ll))
        assert_arr_almost_eq(r_inverted, [east, north], 3)

        r_east, r_north = osgb.transform_point(lon, lat, ll)
        r_inverted = np.array(ll.transform_point(r_east, r_north, osgb))
        assert_arr_almost_eq(r_inverted, [lon, lat])

    @pytest.mark.parametrize('approx', [True, False])
    def test_osgb(self, approx):
        self._check_osgb(ccrs.OSGB(approx=approx))

    def test_epsg(self):
        uk = ccrs.epsg(27700)
        assert uk.epsg_code == 27700
        expected_x = (-104009.357, 688806.007)
        expected_y = (-8908.37, 1256558.45)
        expected_threshold = 7928.15
        if pyproj.__proj_version__ >= '9.2.0':
            expected_x = (-104728.764, 688806.007)
            expected_y = (-8908.36, 1256616.32)
            expected_threshold = 7935.34
        assert_almost_equal(uk.x_limits,
                            expected_x, decimal=3)
        assert_almost_equal(uk.y_limits, expected_y, decimal=2)
        assert_almost_equal(uk.threshold, expected_threshold, decimal=2)
        self._check_osgb(uk)

    def test_epsg_compound_crs(self):
        projection = ccrs.epsg(5973)
        assert projection.epsg_code == 5973

    def test_europp(self):
        europp = ccrs.EuroPP()
        proj4_init = europp.proj4_init
        # Transverse Mercator, UTM zone 32,
        assert '+proj=utm' in proj4_init
        assert '+zone=32' in proj4_init
        # International 1924 ellipsoid.
        assert '+ellps=intl' in proj4_init

    def test_transform_points_nD(self):
        rlons = np.array([[350., 352., 354.], [350., 352., 354.]])
        rlats = np.array([[-5., -0., 1.], [-4., -1., 0.]])

        src_proj = ccrs.RotatedGeodetic(pole_longitude=178.0,
                                        pole_latitude=38.0)
        target_proj = ccrs.Geodetic()
        res = target_proj.transform_points(x=rlons, y=rlats,
                                           src_crs=src_proj)
        unrotated_lon = res[..., 0]
        unrotated_lat = res[..., 1]

        # Solutions derived by proj direct.
        solx = np.array([[-16.42176094, -14.85892262, -11.90627520],
                         [-16.71055023, -14.58434624, -11.68799988]])
        soly = np.array([[46.00724251, 51.29188893, 52.59101488],
                         [46.98728486, 50.30706042, 51.60004528]])
        assert_arr_almost_eq(unrotated_lon, solx)
        assert_arr_almost_eq(unrotated_lat, soly)

    def test_transform_points_1D(self):
        rlons = np.array([350., 352., 354., 356.])
        rlats = np.array([-5., -0., 5., 10.])

        src_proj = ccrs.RotatedGeodetic(pole_longitude=178.0,
                                        pole_latitude=38.0)
        target_proj = ccrs.Geodetic()
        res = target_proj.transform_points(x=rlons, y=rlats,
                                           src_crs=src_proj)
        unrotated_lon = res[..., 0]
        unrotated_lat = res[..., 1]

        # Solutions derived by proj direct.
        solx = np.array([-16.42176094, -14.85892262,
                         -12.88946157, -10.35078336])
        soly = np.array([46.00724251, 51.29188893,
                         56.55031485, 61.77015703])

        assert_arr_almost_eq(unrotated_lon, solx)
        assert_arr_almost_eq(unrotated_lat, soly)

    def test_transform_points_xyz(self):
        # Test geodetic transforms when using z value
        rx = np.array([2574.32516e3])
        ry = np.array([837.562e3])
        rz = np.array([5761.325e3])

        src_proj = ccrs.Geocentric()
        target_proj = ccrs.Geodetic()

        res = target_proj.transform_points(x=rx, y=ry, z=rz,
                                           src_crs=src_proj)

        glat = res[..., 0]
        glon = res[..., 1]
        galt = res[..., 2]

        # Solution generated by pyproj
        solx = np.array([18.0224043189])
        soly = np.array([64.9796515089])
        solz = np.array([5048.03893734])

        assert_arr_almost_eq(glat, solx)
        assert_arr_almost_eq(glon, soly)
        assert_arr_almost_eq(galt, solz)

    def test_transform_points_180(self):
        # Test that values less than -180 and more than 180
        # get mapped to the -180, 180 interval
        x = np.array([-190, 190])
        y = np.array([0, 0])

        proj = ccrs.PlateCarree()

        res = proj.transform_points(x=x, y=y, src_crs=proj)
        assert_array_equal(res[..., :2], [[170, 0], [-170, 0]])

    def test_globe(self):
        # Ensure the globe affects output.
        rugby_globe = ccrs.Globe(semimajor_axis=9000000,
                                 semiminor_axis=9000000,
                                 ellipse=None)
        footy_globe = ccrs.Globe(semimajor_axis=1000000,
                                 semiminor_axis=1000000,
                                 ellipse=None)

        rugby_moll = ccrs.Mollweide(globe=rugby_globe)
        footy_moll = ccrs.Mollweide(globe=footy_globe)

        rugby_pt = rugby_moll.transform_point(
            10, 10, rugby_moll.as_geodetic(),
        )
        footy_pt = footy_moll.transform_point(
            10, 10, footy_moll.as_geodetic(),
        )

        assert_arr_almost_eq(rugby_pt, (1400915, 1741319), decimal=0)
        assert_arr_almost_eq(footy_pt, (155657, 193479), decimal=0)

    def test_project_point(self):
        point = sgeom.Point([0, 45])
        multi_point = sgeom.MultiPoint([point, sgeom.Point([180, 45])])

        pc = ccrs.PlateCarree()
        pc_rotated = ccrs.PlateCarree(central_longitude=180)

        result = pc_rotated.project_geometry(point, pc)
        assert_arr_almost_eq(result.xy, [[-180.], [45.]])

        result = pc_rotated.project_geometry(multi_point, pc)
        assert isinstance(result, sgeom.MultiPoint)
        assert len(result.geoms) == 2
        assert_arr_almost_eq(result.geoms[0].xy, [[-180.], [45.]])
        assert_arr_almost_eq(result.geoms[1].xy, [[0], [45.]])

    def test_utm(self):
        utm30n = ccrs.UTM(30)
        ll = ccrs.Geodetic()
        lon, lat = np.array([-3.0, 51.5], dtype=np.double)
        east, north = np.array([500000, 5705429.2], dtype=np.double)
        assert_arr_almost_eq(utm30n.transform_point(lon, lat, ll),
                             [east, north],
                             decimal=1)
        assert_arr_almost_eq(ll.transform_point(east, north, utm30n),
                             [lon, lat],
                             decimal=1)
        utm38s = ccrs.UTM(38, southern_hemisphere=True)
        lon, lat = np.array([47.5, -18.92], dtype=np.double)
        east, north = np.array([763316.7, 7906160.8], dtype=np.double)
        assert_arr_almost_eq(utm38s.transform_point(lon, lat, ll),
                             [east, north],
                             decimal=1)
        assert_arr_almost_eq(ll.transform_point(east, north, utm38s),
                             [lon, lat],
                             decimal=1)


@pytest.fixture(params=[
    [ccrs.PlateCarree, {}],
    [ccrs.PlateCarree, dict(central_longitude=1.23)],
    [ccrs.NorthPolarStereo, dict(central_longitude=42.5,
                                 globe=ccrs.Globe(ellipse="helmert"))],
    [ccrs.CRS, dict(proj4_params="3088")],
    [ccrs.epsg, dict(code="3088")]
])
def proj_to_copy(request):
    cls, kwargs = request.param
    return cls(**kwargs)


def test_pickle(proj_to_copy):
    # check that we can pickle a simple CRS
    fh = BytesIO()
    pickle.dump(proj_to_copy, fh)
    fh.seek(0)
    pickled_prj = pickle.load(fh)
    assert proj_to_copy == pickled_prj


def test_deepcopy(proj_to_copy):
    prj_cp = copy.deepcopy(proj_to_copy)
    assert proj_to_copy.proj4_params == prj_cp.proj4_params
    assert proj_to_copy == prj_cp


def test_PlateCarree_shortcut():
    central_lons = [[0, 0], [0, 180], [0, 10], [10, 0], [-180, 180], [
        180, -180]]

    target = [([[-180, -180], [-180, 180]], 0),
              ([[-180, 0], [0, 180]], 180),
              ([[-180, -170], [-170, 180]], 10),
              ([[-180, 170], [170, 180]], -10),
              ([[-180, 180], [180, 180]], 360),
              ([[-180, -180], [-180, 180]], -360),
              ]

    assert len(target) == len(central_lons)

    for expected, (s_lon0, t_lon0) in zip(target, central_lons):
        expected_bboxes, expected_offset = expected

        src = ccrs.PlateCarree(central_longitude=s_lon0)
        target = ccrs.PlateCarree(central_longitude=t_lon0)

        bbox, offset = src._bbox_and_offset(target)

        assert offset == expected_offset
        assert bbox == expected_bboxes


def test_transform_points_empty():
    """Test CRS.transform_points with empty array."""
    crs = ccrs.Stereographic()
    result = crs.transform_points(ccrs.PlateCarree(),
                                  np.array([]), np.array([]))
    assert_array_equal(result, np.array([], dtype=np.float64).reshape(0, 3))


def test_transform_points_outside_domain():
    """Test CRS.transform_points with out of domain arrays."""
    # Length-1 arrays error out with a bad status code, while
    # greater than 1 arrays put infinity into the return array
    # where the bad values occur
    crs = ccrs.Orthographic()
    result = crs.transform_points(ccrs.PlateCarree(),
                                  np.array([-120]), np.array([80]))
    assert np.all(np.isnan(result))
    result = crs.transform_points(ccrs.PlateCarree(),
                                  np.array([-120]), np.array([80]),
                                  trap=True)
    assert np.all(np.isnan(result))
    # A length-2 array of the same transform produces "inf" rather
    # than nan due to PROJ never returning nan itself.
    result = crs.transform_points(ccrs.PlateCarree(),
                                  np.array([-120, -120]), np.array([80, 80]))
    assert np.all(~np.isfinite(result[..., :2]))

    # Test singular transform to make sure it is producing all nan's
    # the same as the transform_points call with a length-1 array
    result = crs.transform_point(-120, 80, ccrs.PlateCarree())
    assert np.all(np.isnan(result))


def test_projection__from_string():
    crs = ccrs.Projection("NAD83 / Pennsylvania South")
    assert crs.as_geocentric().datum.name == "North American Datum 1983"
    assert_almost_equal(
        crs.bounds,
        [361633.1351868, 859794.6690229, 45575.5693199, 209415.9845754],
    )


def test_crs__from_pyproj_crs():
    assert ccrs.CRS(pyproj.CRS("EPSG:4326")) == "EPSG:4326"


def test_transform_point_no_warning():
    # Make sure we aren't warning on single-point numpy arrays
    # see https://github.com/SciTools/cartopy/pull/2194
    p = ccrs.PlateCarree()
    p2 = ccrs.Mercator()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        p2.transform_point(1, 2, p)


def test_geographic_bounds_no_area_of_use():
    # Should default to global bounds if no area of use
    wkt = ('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",'
           'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
           'PRIMEM["Greenwich",0.0],UNIT["Degree",0.017453292519943295]]')
    p = pyproj.CRS.from_wkt(wkt)
    assert p.is_geographic
    assert p.area_of_use is None
    p_cartopy = ccrs.Projection(p)
    assert p_cartopy.bounds == (-180, 180, -90, 90)


def test_geographic_bounds_with_area_of_use():
    # Should default to area of use bounds if available
    p = pyproj.CRS.from_epsg(4267)
    assert p.is_geographic
    assert p.area_of_use is not None
    p_cartopy = ccrs.Projection(p)
    x0, x1, y0, y1 = p_cartopy.bounds
    assert_array_equal((x1, y0, x0, y1), p.area_of_use.bounds)
