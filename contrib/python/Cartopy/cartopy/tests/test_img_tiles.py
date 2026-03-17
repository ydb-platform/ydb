# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import hashlib
import os
import types
import warnings

import numpy as np
from numpy.testing import assert_array_almost_equal as assert_arr_almost
import pytest
import shapely.geometry as sgeom

from cartopy import config
import cartopy.crs as ccrs
import cartopy.io.img_tiles as cimgt
import cartopy.io.ogc_clients as ogc


RESOLUTION = (30, 30)

#: Maps Google tile coordinates to native mercator coordinates as defined
#: by https://goo.gl/pgJi.
KNOWN_EXTENTS = {(0, 0, 0): (-20037508.342789244, 20037508.342789244,
                             -20037508.342789244, 20037508.342789244),
                 (2, 0, 2): (0., 10018754.17139462,
                             10018754.17139462, 20037508.342789244),
                 (0, 2, 2): (-20037508.342789244, -10018754.171394622,
                             -10018754.171394622, 0),
                 (2, 2, 2): (0, 10018754.17139462,
                             -10018754.171394622, 0),
                 (8, 9, 4): (0, 2504688.542848654,
                             -5009377.085697312, -2504688.542848654),
                 }


def GOOGLE_IMAGE_URL_REPLACEMENT(self, tile):
    # TODO: This is a hack to replace the Google image URL with a static image.
    #       This service has been deprecated by Google, so we need to replace it
    #       See https://developers.google.com/chart/image for the notice
    pytest.xfail(reason="Google has deprecated the tile API used in this test")

    x, y, z = tile
    return (f'https://chart.googleapis.com/chart?chst=d_text_outline&'
            f'chs=256x256&chf=bg,s,00000055&chld=FFFFFF|16|h|000000|b||||'
            f'Google:%20%20({x},{y})|Zoom%20{z}||||||'
            f'____________________________')


def test_google_tile_styles():
    """
    Tests that setting the Google Maps tile style works as expected.

    This is essentially just assures information is properly propagated through
    the class structure.
    """
    reference_url = ("https://mts0.google.com/vt/lyrs={style}@177000000&hl=en"
                     "&src=api&x=1&y=2&z=3&s=G")
    tile = ["1", "2", "3"]

    # Default is street.
    gt = cimgt.GoogleTiles()
    url = gt._image_url(tile)
    assert reference_url.format(style="m") == url

    # Street
    gt = cimgt.GoogleTiles(style="street")
    url = gt._image_url(tile)
    assert reference_url.format(style="m") == url

    # Satellite
    gt = cimgt.GoogleTiles(style="satellite")
    url = gt._image_url(tile)
    assert reference_url.format(style="s") == url

    # Terrain
    gt = cimgt.GoogleTiles(style="terrain")
    url = gt._image_url(tile)
    assert reference_url.format(style="t") == url

    # Streets only
    gt = cimgt.GoogleTiles(style="only_streets")
    url = gt._image_url(tile)
    assert reference_url.format(style="h") == url

    # Exception is raised if unknown style is passed.
    with pytest.raises(ValueError):
        cimgt.GoogleTiles(style="random_style")


def test_google_wts():
    gt = cimgt.GoogleTiles()

    ll_target_domain = sgeom.box(-15, 50, 0, 60)
    multi_poly = gt.crs.project_geometry(ll_target_domain, ccrs.PlateCarree())
    target_domain = multi_poly.geoms[0]

    with pytest.raises(AssertionError):
        list(gt.find_images(target_domain, -1))
    assert (tuple(gt.find_images(target_domain, 0)) ==
            ((0, 0, 0),))
    assert (tuple(gt.find_images(target_domain, 2)) ==
            ((1, 1, 2), (2, 1, 2)))

    assert (list(gt.subtiles((0, 0, 0))) ==
            [(0, 0, 1), (0, 1, 1), (1, 0, 1), (1, 1, 1)])
    assert (list(gt.subtiles((1, 0, 1))) ==
            [(2, 0, 2), (2, 1, 2), (3, 0, 2), (3, 1, 2)])

    with pytest.raises(AssertionError):
        gt.tileextent((0, 1, 0))

    assert_arr_almost(gt.tileextent((0, 0, 0)), KNOWN_EXTENTS[(0, 0, 0)])
    assert_arr_almost(gt.tileextent((2, 0, 2)), KNOWN_EXTENTS[(2, 0, 2)])
    assert_arr_almost(gt.tileextent((0, 2, 2)), KNOWN_EXTENTS[(0, 2, 2)])
    assert_arr_almost(gt.tileextent((2, 2, 2)), KNOWN_EXTENTS[(2, 2, 2)])
    assert_arr_almost(gt.tileextent((8, 9, 4)), KNOWN_EXTENTS[(8, 9, 4)])


def test_tile_bbox_y0_at_south_pole():
    tms = cimgt.MapQuestOpenAerial()

    # Check the y0_at_north_pole keywords returns the appropriate bounds.
    assert_arr_almost(tms.tile_bbox(8, 6, 4, y0_at_north_pole=False),
                      np.array(KNOWN_EXTENTS[(8, 9, 4)]).reshape([2, 2]))


def test_tile_find_images():
    gt = cimgt.GoogleTiles()
    # Test the find_images method on a GoogleTiles instance.
    ll_target_domain = sgeom.box(-10, 50, 10, 60)
    multi_poly = gt.crs.project_geometry(ll_target_domain, ccrs.PlateCarree())
    target_domain = multi_poly.geoms[0]

    assert (list(gt.find_images(target_domain, 4)) ==
            [(7, 4, 4), (7, 5, 4), (8, 4, 4), (8, 5, 4)])


@pytest.mark.network
def test_image_for_domain():
    gt = cimgt.GoogleTiles()
    gt._image_url = types.MethodType(GOOGLE_IMAGE_URL_REPLACEMENT, gt)

    ll_target_domain = sgeom.box(-10, 50, 10, 60)
    multi_poly = gt.crs.project_geometry(ll_target_domain, ccrs.PlateCarree())
    target_domain = multi_poly.geoms[0]

    _, extent, _ = gt.image_for_domain(target_domain, 6)

    ll_extent = ccrs.Geodetic().transform_points(gt.crs,
                                                 np.array(extent[:2]),
                                                 np.array(extent[2:]))
    assert_arr_almost(ll_extent[:, :2],
                      [[-11.25, 48.92249926],
                       [11.25, 61.60639637]])


def test_quadtree_wts():
    qt = cimgt.QuadtreeTiles()

    ll_target_domain = sgeom.box(-15, 50, 0, 60)
    multi_poly = qt.crs.project_geometry(ll_target_domain, ccrs.PlateCarree())
    target_domain = multi_poly.geoms[0]

    with pytest.raises(ValueError):
        list(qt.find_images(target_domain, 0))

    assert qt.tms_to_quadkey((1, 1, 1)) == '1'
    assert qt.quadkey_to_tms('1') == (1, 1, 1)

    assert qt.tms_to_quadkey((8, 9, 4)) == '1220'
    assert qt.quadkey_to_tms('1220') == (8, 9, 4)

    assert tuple(qt.find_images(target_domain, 1)) == ('0', '1')
    assert tuple(qt.find_images(target_domain, 2)) == ('03', '12')

    assert list(qt.subtiles('0')) == ['00', '01', '02', '03']
    assert list(qt.subtiles('11')) == ['110', '111', '112', '113']

    with pytest.raises(ValueError):
        qt.tileextent('4')

    assert_arr_almost(qt.tileextent(''), KNOWN_EXTENTS[(0, 0, 0)])
    assert_arr_almost(qt.tileextent(qt.tms_to_quadkey((2, 0, 2), google=True)),
                      KNOWN_EXTENTS[(2, 0, 2)])
    assert_arr_almost(qt.tileextent(qt.tms_to_quadkey((0, 2, 2), google=True)),
                      KNOWN_EXTENTS[(0, 2, 2)])
    assert_arr_almost(qt.tileextent(qt.tms_to_quadkey((2, 0, 2), google=True)),
                      KNOWN_EXTENTS[(2, 0, 2)])
    assert_arr_almost(qt.tileextent(qt.tms_to_quadkey((2, 2, 2), google=True)),
                      KNOWN_EXTENTS[(2, 2, 2)])
    assert_arr_almost(qt.tileextent(qt.tms_to_quadkey((8, 9, 4), google=True)),
                      KNOWN_EXTENTS[(8, 9, 4)])


def test_mapbox_tiles_api_url():
    token = 'foo'
    map_name = 'bar'
    tile = [0, 1, 2]
    exp_url = ('https://api.mapbox.com/styles/v1/mapbox/bar/tiles'
               '/2/0/1?access_token=foo')

    mapbox_sample = cimgt.MapboxTiles(token, map_name)
    url_str = mapbox_sample._image_url(tile)
    assert url_str == exp_url


def test_mapbox_style_tiles_api_url():
    token = 'foo'
    username = 'baz'
    map_id = 'bar'
    tile = [0, 1, 2]
    exp_url = ('https://api.mapbox.com/styles/v1/'
               'baz/bar/tiles/256/2/0/1'
               '?access_token=foo')

    mapbox_sample = cimgt.MapboxStyleTiles(token, username, map_id)
    url_str = mapbox_sample._image_url(tile)
    assert url_str == exp_url


@pytest.mark.parametrize("style,extension,resolution", [
    ("alidade_smooth", "png", ""),
    ("alidade_smooth", "png", "@2x"),
    ("stamen_watercolor", "jpg", "")])
def test_stadia_maps_tiles_api_url(style, extension, resolution):
    apikey = 'foo'
    tile = [0, 1, 2]
    exp_url = ('http://tiles.stadiamaps.com/tiles/'
               f'{style}/2/0/1{resolution}.{extension}'
               '?api_key=foo')

    sample = cimgt.StadiaMapsTiles(apikey, style=style, resolution=resolution)
    url_str = sample._image_url(tile)
    assert url_str == exp_url


def test_ordnance_survey_tile_styles():
    """
    Tests that setting the Ordnance Survey tile style works as expected.

    This is essentially just assures information is properly propagated through
    the class structure.
    """
    dummy_apikey = "None"

    ref_url = "https://api.os.uk/maps/raster/v1/zxy/" \
              "{layer}/{z}/{x}/{y}.png?key=None"
    tile = ["1", "2", "3"]

    # Default is Road_3857.
    ordsurvey = cimgt.OrdnanceSurvey(dummy_apikey)
    url = ordsurvey._image_url(tile)
    assert url == ref_url.format(layer="Road_3857",
                                 z=tile[2], y=tile[1], x=tile[0])

    for layer in ("Road_3857", "Light_3857", "Outdoor_3857",
                  "Road", "Light", "Outdoor"):
        ordsurvey = cimgt.OrdnanceSurvey(dummy_apikey, layer=layer)
        url = ordsurvey._image_url(tile)
        layer = layer if layer.endswith("_3857") else layer + "_3857"
        assert url == ref_url.format(layer=layer,
                                     z=tile[2], y=tile[1], x=tile[0])

    # Exception is raised if unknown style is passed.
    with pytest.raises(ValueError):
        cimgt.OrdnanceSurvey(dummy_apikey, layer="random_style")


@pytest.mark.network
def test_ordnance_survey_get_image():
    # In order to test fetching map images from OS
    # an API key needs to be provided
    try:
        api_key = os.environ['ORDNANCE_SURVEY_API_KEY']
    except KeyError:
        pytest.skip('ORDNANCE_SURVEY_API_KEY environment variable is unset.')

    os1 = cimgt.OrdnanceSurvey(api_key, layer="Outdoor_3857")
    os2 = cimgt.OrdnanceSurvey(api_key, layer="Light_3857")

    tile = (500, 300, 10)

    img1, extent1, _ = os1.get_image(tile)
    img2, extent2, _ = os2.get_image(tile)

    # Different images for different layers
    assert img1 != img2

    # The extent is the same though
    assert extent1 == extent2


def test_azuremaps_tiles_api_url():
    subscription_key = 'foo'
    tileset_id = 'bar'
    tile = [0, 1, 2]

    exp_url = ('https://atlas.microsoft.com/map/tile?api-version=2.0'
               '&tilesetId=bar&x=0&y=1&zoom=2&subscription-key=foo')

    az_maps_sample = cimgt.AzureMapsTiles(subscription_key, tileset_id)
    url_str = az_maps_sample._image_url(tile)
    assert url_str == exp_url


@pytest.mark.network
def test_azuremaps_get_image():
    # In order to test fetching map images from Azure Maps
    # an API key needs to be provided
    try:
        api_key = os.environ['AZURE_MAPS_SUBSCRIPTION_KEY']
    except KeyError:
        pytest.skip('AZURE_MAPS_SUBSCRIPTION_KEY environment variable '
                    'is unset.')

    am1 = cimgt.AzureMapsTiles(api_key, tileset_id="microsoft.imagery")
    am2 = cimgt.AzureMapsTiles(api_key, tileset_id="microsoft.base.road")

    tile = (500, 300, 10)

    img1, extent1, _ = am1.get_image(tile)
    img2, extent2, _ = am2.get_image(tile)

    # Different images for different layers
    assert img1 != img2

    # The extent is the same though
    assert extent1 == extent2


@pytest.mark.network
@pytest.mark.parametrize('cache_dir', ["tmpdir", True, False])
@pytest.mark.skipif(not ogc._OWSLIB_AVAILABLE, reason='OWSLib is unavailable.')
def test_wmts_cache(cache_dir, tmp_path):
    if cache_dir == "tmpdir":
        tmpdir_str = str(tmp_path)
    else:
        tmpdir_str = cache_dir

    if cache_dir is True:
        config["cache_dir"] = str(tmp_path)

    # URI = 'https://map1c.vis.earthdata.nasa.gov/wmts-geo/wmts.cgi'
    # layer_name = 'VIIRS_CityLights_2012'
    URI = 'https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/WMTS/1.0.0/WMTSCapabilities.xml'
    layer_name='USGSImageryOnly'
    projection = ccrs.PlateCarree()

    # Fetch tiles and save them in the cache
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        source = ogc.WMTSRasterSource(URI, layer_name, cache=tmpdir_str)
    extent = [-10, 10, 40, 60]
    located_image, = source.fetch_raster(projection, extent,
                                         RESOLUTION)

    # Do not check the result if the cache is disabled
    if cache_dir is False:
        assert source.cache_path is None
        return

    # Check that the warning is properly raised (only when cache is True)
    if cache_dir is True:
        assert len(w) == 1
    else:
        assert len(w) == 0

    # Define expected results
    x_y_f_h = [
        (1, 1, '1_1.npy', '0de548bd47e4579ae0500da6ceeb08e7'),
        (1, 2, '1_2.npy', '4beebcd3e4408af5accb440d7b4c8933'),
    ]

    # Check the results
    cache_dir_res = source.cache_path / "WMTSRasterSource"
    files = list(cache_dir_res.iterdir())
    hashes = {
        f:
            hashlib.md5(
                np.load(cache_dir_res / f, allow_pickle=True).data
            ).hexdigest()
        for f in files
    }
    assert sorted(files) == [cache_dir_res / f for x, y, f, h in x_y_f_h]
    assert set(files) == set([cache_dir_res / c for c in source.cache])

    assert sorted(hashes.values()) == sorted(
        h for x, y, f, h in x_y_f_h
    )

    # Update images in cache (all white)
    for f in files:
        filename = cache_dir_res / f
        img = np.load(filename, allow_pickle=True)
        img.fill(255)
        np.save(filename, img, allow_pickle=True)

    wmts_cache = ogc.WMTSRasterSource(URI, layer_name, cache=tmpdir_str)
    located_image_cache, = wmts_cache.fetch_raster(projection, extent,
                                         RESOLUTION)

    # Check that the new fetch_raster() call used cached images
    assert wmts_cache.cache == set([cache_dir_res / c for c in source.cache])
    assert (np.array(located_image_cache.image) == 255).all()


@pytest.mark.network
@pytest.mark.parametrize('cache_dir', ["tmpdir", True, False])
def test_cache(cache_dir, tmp_path):
    if cache_dir == "tmpdir":
        tmpdir_str = str(tmp_path)
    else:
        tmpdir_str = cache_dir

    if cache_dir is True:
        config["cache_dir"] = str(tmp_path)

    # Fetch tiles and save them in the cache
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        gt = cimgt.GoogleTiles(cache=tmpdir_str)
    gt._image_url = types.MethodType(GOOGLE_IMAGE_URL_REPLACEMENT, gt)

    ll_target_domain = sgeom.box(-10, 50, 10, 60)
    multi_poly = gt.crs.project_geometry(ll_target_domain, ccrs.PlateCarree())
    target_domain = multi_poly.geoms[0]

    img_init, _, _ = gt.image_for_domain(target_domain, 6)

    # Do not check the result if the cache is disabled
    if cache_dir is False:
        assert gt.cache_path is None
        return

    # Check that the warning is properly raised (only when cache is True)
    if cache_dir is True:
        assert len(w) == 1
    else:
        assert len(w) == 0

    # Define expected results
    x_y_z_f_h = [
        (30, 18, 6, '30_18_6.npy', '545db25f1aa348ad85e1f437fd0db0d9'),
        (30, 19, 6, '30_19_6.npy', '10355add0674bfa33f673ea27a6d1206'),
        (30, 20, 6, '30_20_6.npy', 'ab3e7f2ed8d71977ac176094973695ae'),
        (30, 21, 6, '30_21_6.npy', '3e8947b93a6ffa07f22cfea4042a4740'),
        (31, 18, 6, '31_18_6.npy', 'd0fa58b9146aa99b273eb75256b328cc'),
        (31, 19, 6, '31_19_6.npy', '9255bd0cd22736bd2c25a9087bd47b20'),
        (31, 20, 6, '31_20_6.npy', 'ac0f7e32bdf8edb50d1dccf3ec0ef446'),
        (31, 21, 6, '31_21_6.npy', 'f36b8cc1825bf267b2daead837facae9'),
        (32, 18, 6, '32_18_6.npy', '9f4ddd90cd1ae76ef2bbc8f0252ead91'),
        (32, 19, 6, '32_19_6.npy', 'a995803578bb94ecfca8563754717196'),
        (32, 20, 6, '32_20_6.npy', 'def9e71d77fd6007c77c2a14dfae858f'),
        (32, 21, 6, '32_21_6.npy', 'a3d7935037019ec58ae78f60e6fb924e'),
        (33, 18, 6, '33_18_6.npy', '4e51e32da73fb99229817dcd7b7e1f4f'),
        (33, 19, 6, '33_19_6.npy', 'b9b5057fa012c5788cbbe1e18c9bb512'),
        (33, 20, 6, '33_20_6.npy', 'b55a7c0a8d86167df496732f85bddcf9'),
        (33, 21, 6, '33_21_6.npy', '4208ba897c460e9bb0d2469552e127ff')
    ]

    # Check the results
    cache_dir_res = gt.cache_path / "GoogleTiles"
    files = list(cache_dir_res.iterdir())
    hashes = {
        f:
            hashlib.md5(
                np.load(cache_dir_res / f, allow_pickle=True).data
            ).hexdigest()
        for f in files
    }
    assert sorted(files) == [cache_dir_res / f for x, y, z, f, h in x_y_z_f_h]
    assert set(files) == gt.cache

    assert sorted(hashes.values()) == sorted(
        h for x, y, z, f, h in x_y_z_f_h
    )

    # Update images in cache (all white)
    for f in files:
        filename = cache_dir_res / f
        img = np.load(filename, allow_pickle=True)
        img.fill(255)
        np.save(filename, img, allow_pickle=True)

    gt_cache = cimgt.GoogleTiles(cache=tmpdir_str)
    gt_cache._image_url = types.MethodType(
        GOOGLE_IMAGE_URL_REPLACEMENT, gt_cache)
    img_cache, _, _ = gt_cache.image_for_domain(target_domain, 6)

    # Check that the new image_for_domain() call used cached images
    assert gt_cache.cache == gt.cache
    assert (img_cache == 255).all()
