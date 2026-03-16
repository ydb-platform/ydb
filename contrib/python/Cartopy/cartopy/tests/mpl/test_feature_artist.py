# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.


import matplotlib.colors as mcolors
import matplotlib.path as mpath
import matplotlib.pyplot as plt
import numpy as np
import pytest
import shapely.geometry as sgeom

import cartopy.crs as ccrs
from cartopy.feature import ShapelyFeature
from cartopy.mpl.feature_artist import FeatureArtist, _freeze, _GeomKey


@pytest.mark.parametrize("source, expected", [
    [{1: 0}, frozenset({(1, 0)})],
    [[1, 2], (1, 2)],
    [[1, {}], (1, frozenset())],
    [[1, {'a': [1, 2, 3]}], (1, frozenset([('a', (1, 2, 3))]))],
    [{'edgecolor': 'face', 'zorder': -1,
      'facecolor': np.array([0.9375, 0.9375, 0.859375])},
     frozenset([('edgecolor', 'face'), ('zorder', -1),
                ('facecolor', (0.9375, 0.9375, 0.859375))])],
])
def test_freeze(source, expected):
    assert _freeze(source) == expected


@pytest.fixture
def feature():
    circle1 = sgeom.Point(0, 0).buffer(1)
    circle2 = sgeom.Point(0, 0).buffer(10)
    square = sgeom.Polygon([(30, 0), (50, 0), (50, 20), (30, 20), (30, 0)])
    geoms = [circle1, circle2, square]
    feature = ShapelyFeature(geoms, ccrs.PlateCarree())
    return feature


def robinson_map():
    """
    Set up a common map for the image tests.  The extent is chosen to include
    only the square geometry from `feature`.  This means that we can check that
    `array` or a list of facecolors remains 1-to-1 with the list of geometries.
    """
    prj_crs = ccrs.Robinson()
    fig, ax = plt.subplots(subplot_kw={'projection':prj_crs})
    ax.set_extent([20, 180, -90, 90])
    ax.coastlines()

    return fig, ax


def cached_paths(geom, target_projection):
    # Use the cache in FeatureArtist to get back the projected path
    # for the given geometry.
    geom_cache = FeatureArtist._geom_key_to_path_cache.get(_GeomKey(geom), {})
    return geom_cache.get(target_projection, None)


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='feature_artist.png')
def test_feature_artist_draw(feature):
    fig, ax = robinson_map()
    ax.add_feature(feature, facecolor='blue')


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='feature_artist.png')
def test_feature_artist_draw_facecolor_list(feature):
    fig, ax = robinson_map()
    ax.add_feature(feature, facecolor=['red', 'green', 'blue'])


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='feature_artist.png')
def test_feature_artist_draw_cmap(feature):
    fig, ax = robinson_map()

    cmap = mcolors.ListedColormap(['red', 'gray', 'blue'])
    ax.add_feature(feature, cmap=cmap, array=[0, 0, 1])


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='feature_artist.png')
def test_feature_artist_draw_styled_feature(feature):
    geoms = list(feature.geometries())
    styled_feature = ShapelyFeature(geoms, crs=ccrs.PlateCarree(), facecolor='blue')

    fig, ax = robinson_map()
    ax.add_feature(styled_feature)


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='feature_artist.png')
def test_feature_artist_draw_styler(feature):
    geoms = list(feature.geometries())

    def styler(geom):
        if geom == geoms[1]:
            return {'facecolor': 'red'}
        else:
            return {'facecolor':'blue'}

    fig, ax = robinson_map()
    ax.add_feature(feature, facecolor='red', styler=styler)


def test_feature_artist_geom_single_path(feature):
    plot_crs = ccrs.PlateCarree(central_longitude=180)
    fig, ax = plt.subplots(subplot_kw={'projection': plot_crs})
    ax.add_feature(feature)

    fig.draw_without_rendering()

    # Circles get split into two geometries across the dateline, but should still be
    # plotted as one compound path to ensure style consistency.
    for geom in feature.geometries():
        assert isinstance(cached_paths(geom, plot_crs), mpath.Path)
