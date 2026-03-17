# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import gc
from unittest import mock


try:
    from owslib.wmts import WebMapTileService
except ImportError:
    WebMapTileService = None
import matplotlib.pyplot as plt
import numpy as np
import pytest

import cartopy.crs as ccrs
from cartopy.tests.conftest import _HAS_PYKDTREE_OR_SCIPY


if _HAS_PYKDTREE_OR_SCIPY:
    from cartopy.io.ogc_clients import _OWSLIB_AVAILABLE, WMTSRasterSource

import cartopy.io.shapereader
from cartopy.mpl import _MPL_38
from cartopy.mpl.feature_artist import FeatureArtist
import cartopy.mpl.geoaxes as cgeoaxes
import cartopy.mpl.path


def sample_data(shape=(73, 145)):
    """Return ``lons``, ``lats`` and ``data`` of some fake data."""
    nlats, nlons = shape
    lats = np.linspace(-np.pi / 2, np.pi / 2, nlats, dtype=np.longdouble)
    lons = np.linspace(0, 2 * np.pi, nlons, dtype=np.longdouble)
    lons, lats = np.meshgrid(lons, lats)
    wave = 0.75 * (np.sin(2 * lats) ** 8) * np.cos(4 * lons)
    mean = 0.5 * np.cos(2 * lats) * ((np.sin(2 * lats)) ** 2 + 2)

    lats = np.rad2deg(lats)
    lons = np.rad2deg(lons)
    data = wave + mean

    return lons, lats, data


@pytest.mark.natural_earth
def test_coastline_loading_cache():
    # a5caae040ee11e72a62a53100fe5edc355304419 added coastline caching.
    # This test ensures it is working.
    fig = plt.figure()

    # Create coastlines to ensure they are cached.
    ax1 = fig.add_subplot(2, 1, 1, projection=ccrs.PlateCarree())
    ax1.coastlines()
    fig.canvas.draw()
    # Create another instance of the coastlines and count
    # the number of times shapereader.Reader is created.
    with mock.patch('cartopy.io.shapereader.Reader.__init__',
                    return_value=None) as counter:
        ax2 = fig.add_subplot(2, 1, 1, projection=ccrs.Robinson())
        ax2.coastlines()
        fig.canvas.draw()
    counter.assert_not_called()


@pytest.mark.natural_earth
def test_shapefile_transform_cache():
    # a5caae040ee11e72a62a53100fe5edc355304419 added shapefile mpl
    # geometry caching based on geometry object id. This test ensures
    # it is working.
    coastline_path = cartopy.io.shapereader.natural_earth(resolution="110m",
                                                          category='physical',
                                                          name='coastline')
    geoms = cartopy.io.shapereader.Reader(coastline_path).geometries()
    # Use the first 10 of them.
    geoms = tuple(geoms)[:10]
    n_geom = len(geoms)

    fig = plt.figure()
    ax = plt.axes(projection=ccrs.Robinson())

    # Empty the cache.
    FeatureArtist._geom_key_to_geometry_cache.clear()
    FeatureArtist._geom_key_to_path_cache.clear()
    assert len(FeatureArtist._geom_key_to_geometry_cache) == 0
    assert len(FeatureArtist._geom_key_to_path_cache) == 0

    with mock.patch.object(ax.projection, 'project_geometry',
                           wraps=ax.projection.project_geometry) as counter:
        ax.add_geometries(geoms, ccrs.PlateCarree())
        ax.add_geometries(geoms, ccrs.PlateCarree())
        ax.add_geometries(geoms[:], ccrs.PlateCarree())
        fig.canvas.draw()

    # Without caching the count would have been
    # n_calls * n_geom, but should now be just n_geom.
    assert counter.call_count == n_geom, (
        f'The given geometry was transformed too many times (expected: '
        f'{n_geom}; got {counter.call_count}) - the caching is not working.')

    # Check the cache has an entry for each geometry.
    assert len(FeatureArtist._geom_key_to_geometry_cache) == n_geom
    assert len(FeatureArtist._geom_key_to_path_cache) == n_geom

    # Check that the cache is empty again once we've dropped all references
    # to the source paths.
    fig.clf()
    del geoms, counter
    gc.collect()
    assert len(FeatureArtist._geom_key_to_geometry_cache) == 0
    assert len(FeatureArtist._geom_key_to_path_cache) == 0


def test_contourf_transform_path_counting():
    fig = plt.figure()
    ax = plt.axes(projection=ccrs.Robinson())
    fig.canvas.draw()

    # Capture the size of the cache before our test.
    gc.collect()
    initial_cache_size = len(cgeoaxes._PATH_TRANSFORM_CACHE)

    with mock.patch('cartopy.mpl.path.path_to_shapely',
                    wraps=cartopy.mpl.path.path_to_shapely) as path_to_shapely_counter:
        x, y, z = sample_data((30, 60))
        cs = ax.contourf(x, y, z, 5, transform=ccrs.PlateCarree())
        if not _MPL_38:
            n_geom = sum(len(c.get_paths()) for c in cs.collections)
        else:
            n_geom = len(cs.get_paths())

        del cs
        fig.canvas.draw()

    # Before the performance enhancement, the count would have been 2 * n_geom,
    # but should now be just n_geom.
    assert path_to_shapely_counter.call_count == n_geom, (
        f'The given geometry was transformed too many times (expected: '
        f'{n_geom}; got {path_to_shapely_counter.call_count}) - the caching is '
        f'not working.')

    # Check the cache has an entry for each geometry.
    assert len(cgeoaxes._PATH_TRANSFORM_CACHE) == initial_cache_size + n_geom

    # Check that the cache is empty again once we've dropped all references
    # to the source paths.
    fig.clf()
    del path_to_shapely_counter
    gc.collect()
    assert len(cgeoaxes._PATH_TRANSFORM_CACHE) == initial_cache_size


@pytest.mark.filterwarnings("ignore:TileMatrixLimits")
@pytest.mark.network
@pytest.mark.skipif(not _HAS_PYKDTREE_OR_SCIPY or not _OWSLIB_AVAILABLE,
                    reason='OWSLib and at least one of pykdtree or scipy is required')
@pytest.mark.xfail(reason='NASA servers are returning bad content metadata')
def test_wmts_tile_caching():
    image_cache = WMTSRasterSource._shared_image_cache
    image_cache.clear()
    assert len(image_cache) == 0

    url = 'https://map1c.vis.earthdata.nasa.gov/wmts-geo/wmts.cgi'
    wmts = WebMapTileService(url)
    layer_name = 'MODIS_Terra_CorrectedReflectance_TrueColor'

    source = WMTSRasterSource(wmts, layer_name)

    crs = ccrs.PlateCarree()
    extent = (-180, 180, -90, 90)
    resolution = (20, 10)
    n_tiles = 2
    with mock.patch.object(wmts, 'gettile',
                           wraps=wmts.gettile) as gettile_counter:
        source.fetch_raster(crs, extent, resolution)
    assert gettile_counter.call_count == n_tiles, (
        f'Too many tile requests - expected {n_tiles}, got '
        f'{gettile_counter.call_count}.')
    del gettile_counter
    gc.collect()
    assert len(image_cache) == 1
    assert len(image_cache[wmts]) == 1
    tiles_key = (layer_name, '0')
    assert len(image_cache[wmts][tiles_key]) == n_tiles

    # Second time around we shouldn't request any more tiles.
    with mock.patch.object(wmts, 'gettile',
                           wraps=wmts.gettile) as gettile_counter:
        source.fetch_raster(crs, extent, resolution)
    gettile_counter.assert_not_called()
    del gettile_counter
    gc.collect()
    assert len(image_cache) == 1
    assert len(image_cache[wmts]) == 1
    tiles_key = (layer_name, '0')
    assert len(image_cache[wmts][tiles_key]) == n_tiles

    # Once there are no live references the weak-ref cache should clear.
    del source, wmts
    gc.collect()
    assert len(image_cache) == 0
