# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from matplotlib.collections import PatchCollection
import matplotlib.patches as mpatches
from matplotlib.path import Path
import matplotlib.pyplot as plt
import numpy as np
import pytest
import shapely.geometry as sgeom

import cartopy.crs as ccrs
import cartopy.mpl.patch as cpatch
import cartopy.mpl.path as cpath


# Note: Matplotlib is broken here
# https://github.com/matplotlib/matplotlib/issues/15946
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='poly_interiors.png', tolerance=3.1)
@pytest.mark.parametrize('use_legacy_geos_funcs', [False, True])
def test_polygon_interiors(use_legacy_geos_funcs):
    fig = plt.figure()

    ax = fig.add_subplot(2, 1, 1, projection=ccrs.PlateCarree())
    ax.coastlines()
    ax.set_global()

    pth = Path([[0, -45], [60, -45], [60, 45], [0, 45], [0, 45],
                [10, -20], [10, 20], [40, 20], [40, -20], [10, 20]],
               [1, 2, 2, 2, 79, 1, 2, 2, 2, 79])

    if use_legacy_geos_funcs:
        patches_native = []
        patches = []
        with pytest.warns(DeprecationWarning, match="path_to_geos is deprecated"):
            for geos in cpatch.path_to_geos(pth):
                with pytest.warns(DeprecationWarning, match="geos_to_path is deprecat"):
                    for pth in cpatch.geos_to_path(geos):
                        patches.append(mpatches.PathPatch(pth))

                # buffer by 10 degrees (leaves a small hole in the middle)
                geos_buffered = geos.buffer(10)
                with pytest.warns(DeprecationWarning, match="geos_to_path is deprecat"):
                    for pth in cpatch.geos_to_path(geos_buffered):
                        patches_native.append(mpatches.PathPatch(pth))
    else:
        geom = cpath.path_to_shapely(pth)
        assert isinstance(geom, sgeom.Polygon)
        path = cpath.shapely_to_path(geom)
        patches = [mpatches.PathPatch(path)]

        geom_buffered = geom.buffer(10)
        path_buffered = cpath.shapely_to_path(geom_buffered)
        patches_native = [mpatches.PathPatch(path_buffered)]

    # Set high zorder to ensure the polygons are drawn on top of coastlines.
    collection = PatchCollection(patches_native, facecolor='red', alpha=0.4,
                                 transform=ax.projection, zorder=10)
    ax.add_collection(collection)

    collection = PatchCollection(patches, facecolor='yellow', alpha=0.4,
                                 transform=ccrs.Geodetic(), zorder=10)

    ax.add_collection(collection)

    # test multiple interior polygons
    ax = fig.add_subplot(2, 1, 2, projection=ccrs.PlateCarree(),
                         xlim=[-5, 15], ylim=[-5, 15])
    ax.coastlines(resolution="110m")

    exterior = np.array(sgeom.box(0, 0, 12, 12).exterior.coords)
    interiors = [np.array(sgeom.box(1, 1, 2, 2, ccw=False).exterior.coords),
                 np.array(sgeom.box(1, 8, 2, 9, ccw=False).exterior.coords)]
    poly = sgeom.Polygon(exterior, interiors)

    if use_legacy_geos_funcs:
        patches = []
        with pytest.warns(DeprecationWarning, match="geos_to_path is deprecated"):
            for pth in cpatch.geos_to_path(poly):
                patches.append(mpatches.PathPatch(pth))
    else:
        path = cpath.shapely_to_path(poly)
        patches = [mpatches.PathPatch(path)]

    collection = PatchCollection(patches, facecolor='yellow', alpha=0.4,
                                 transform=ccrs.Geodetic(), zorder=10)
    ax.add_collection(collection)

    return fig


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='contour_with_interiors.png')
def test_contour_interiors():
    # produces a polygon with multiple holes:
    nx, ny = 10, 10
    numlev = 2
    lons, lats = np.meshgrid(np.linspace(-50, 50, nx),
                             np.linspace(-45, 45, ny))
    data = np.sin(np.hypot(lons, lats))
    fig = plt.figure()

    ax = fig.add_subplot(2, 2, 1, projection=ccrs.PlateCarree())
    ax.set_global()
    ax.contourf(lons, lats, data, numlev, transform=ccrs.PlateCarree())
    ax.coastlines()

    ax = fig.add_subplot(2, 2, 2, projection=ccrs.Robinson())
    ax.set_global()
    ax.contourf(lons, lats, data, numlev, transform=ccrs.PlateCarree())
    ax.coastlines()

    # produces singular polygons (zero area polygons)

    numlev = 2
    x, y = np.meshgrid(np.arange(-5.5, 5.5, 0.25), np.arange(-5.5, 5.5, 0.25))
    dim = x.shape[0]
    data = np.sin(np.hypot(x, y))
    lats = np.arange(dim) + 30
    lons = np.arange(dim) - 20

    ax = fig.add_subplot(2, 2, 3, projection=ccrs.PlateCarree())
    ax.set_global()
    ax.contourf(lons, lats, data, numlev, transform=ccrs.PlateCarree())
    ax.coastlines()

    ax = fig.add_subplot(2, 2, 4, projection=ccrs.Robinson())
    ax.set_global()
    ax.contourf(lons, lats, data, numlev, transform=ccrs.PlateCarree())
    ax.coastlines()

    return fig
