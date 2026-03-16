# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
import matplotlib.pyplot as plt
import pytest

import cartopy.crs as ccrs
from cartopy.mpl.ticker import LatitudeFormatter, LongitudeFormatter


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='xticks_no_transform.png')
def test_set_xticks_no_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines('110m')
    ax.xaxis.set_major_formatter(LongitudeFormatter(degree_symbol=''))
    ax.set_xticks([-180, -90, 0, 90, 180])
    ax.set_xticks([-135, -45, 45, 135], minor=True)
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='xticks_cylindrical.png')
def test_set_xticks_cylindrical():
    ax = plt.axes(projection=ccrs.Mercator(min_latitude=-85, max_latitude=85))
    ax.coastlines('110m')
    ax.xaxis.set_major_formatter(LongitudeFormatter(degree_symbol=''))
    ax.set_xticks([-180, -90, 0, 90, 180], crs=ccrs.PlateCarree())
    ax.set_xticks([-135, -45, 45, 135], minor=True, crs=ccrs.PlateCarree())
    return ax.figure


def test_set_xticks_non_cylindrical():
    ax = plt.axes(projection=ccrs.Orthographic())
    with pytest.raises(RuntimeError):
        ax.set_xticks([-180, -90, 0, 90, 180], crs=ccrs.Geodetic())
    with pytest.raises(RuntimeError):
        ax.set_xticks([-135, -45, 45, 135], minor=True, crs=ccrs.Geodetic())


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='yticks_no_transform.png')
def test_set_yticks_no_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines('110m')
    ax.yaxis.set_major_formatter(LatitudeFormatter(degree_symbol=''))
    ax.set_yticks([-60, -30, 0, 30, 60])
    ax.set_yticks([-75, -45, -15, 15, 45, 75], minor=True)
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='yticks_cylindrical.png')
def test_set_yticks_cylindrical():
    ax = plt.axes(projection=ccrs.Mercator(min_latitude=-85, max_latitude=85))
    ax.coastlines('110m')
    ax.yaxis.set_major_formatter(LatitudeFormatter(degree_symbol=''))
    ax.set_yticks([-60, -30, 0, 30, 60], crs=ccrs.PlateCarree())
    ax.set_yticks([-75, -45, -15, 15, 45, 75], minor=True,
                  crs=ccrs.PlateCarree())
    return ax.figure


def test_set_yticks_non_cylindrical():
    ax = plt.axes(projection=ccrs.Orthographic())
    with pytest.raises(RuntimeError):
        ax.set_yticks([-60, -30, 0, 30, 60], crs=ccrs.Geodetic())
    with pytest.raises(RuntimeError):
        ax.set_yticks([-75, -45, -15, 15, 45, 75], minor=True,
                      crs=ccrs.Geodetic())


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='xyticks.png')
def test_set_xyticks():
    fig = plt.figure(figsize=(10, 10))
    projections = (ccrs.PlateCarree(),
                   ccrs.Mercator(),
                   ccrs.TransverseMercator(approx=False))
    x = -3.275024
    y = 50.753998
    for i, prj in enumerate(projections, 1):
        ax = fig.add_subplot(3, 1, i, projection=prj)
        ax.set_extent([-12.5, 4, 49, 60], ccrs.Geodetic())
        ax.coastlines('110m')
        p, q = prj.transform_point(x, y, ccrs.Geodetic())
        ax.set_xticks([p])
        ax.set_yticks([q])
    return fig
