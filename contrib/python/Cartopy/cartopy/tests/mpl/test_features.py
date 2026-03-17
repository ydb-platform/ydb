# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
from xml.etree.ElementTree import ParseError

import matplotlib.pyplot as plt
import pytest

import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.tests.conftest import _HAS_PYKDTREE_OR_SCIPY


if _HAS_PYKDTREE_OR_SCIPY:
    from cartopy.io.ogc_clients import _OWSLIB_AVAILABLE


@pytest.mark.filterwarnings("ignore:Downloading")
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='natural_earth.png')
def test_natural_earth():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.OCEAN)
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.LAKES, alpha=0.5)
    ax.add_feature(cfeature.RIVERS)
    ax.set_xlim((-20, 60))
    ax.set_ylim((-40, 40))
    return ax.figure


@pytest.mark.filterwarnings("ignore:Downloading")
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='natural_earth_custom.png')
def test_natural_earth_custom():
    ax = plt.axes(projection=ccrs.PlateCarree())
    feature = cfeature.NaturalEarthFeature('physical', 'coastline', '50m',
                                           edgecolor='black',
                                           facecolor='none')
    ax.add_feature(feature)
    ax.set_xlim((-26, -12))
    ax.set_ylim((58, 72))
    return ax.figure


@pytest.mark.network
@pytest.mark.skipif(not _HAS_PYKDTREE_OR_SCIPY, reason='pykdtree or scipy is required')
@pytest.mark.mpl_image_compare(filename='gshhs_coastlines.png', tolerance=0.95)
def test_gshhs():
    ax = plt.axes(projection=ccrs.Mollweide())
    ax.set_extent([138, 142, 32, 42], ccrs.Geodetic())

    ax.stock_img()
    # Draw coastlines.
    ax.add_feature(cfeature.GSHHSFeature('coarse', edgecolor='red'))
    # Draw higher resolution lakes (and test overriding of kwargs)
    ax.add_feature(cfeature.GSHHSFeature('low', levels=[2],
                                         facecolor='green'), facecolor='blue')
    return ax.figure


@pytest.mark.network
@pytest.mark.skipif(not _HAS_PYKDTREE_OR_SCIPY or not _OWSLIB_AVAILABLE,
                    reason='OWSLib and at least one of pykdtree or scipy is required')
@pytest.mark.xfail(raises=ParseError,
                   reason="Bad XML returned from the URL")
@pytest.mark.mpl_image_compare(filename='wfs.png')
def test_wfs():
    ax = plt.axes(projection=ccrs.OSGB(approx=True))
    url = 'https://nsidc.org/cgi-bin/atlas_south?service=WFS'
    typename = 'land_excluding_antarctica'
    feature = cfeature.WFSFeature(url, typename,
                                  edgecolor='red')
    ax.add_feature(feature)
    return ax.figure


@pytest.mark.network
@pytest.mark.skipif(not _HAS_PYKDTREE_OR_SCIPY or not _OWSLIB_AVAILABLE,
                    reason='OWSLib and at least one of pykdtree or scipy is required')
@pytest.mark.xfail(raises=(ParseError, AttributeError),
                   reason="Bad XML returned from the URL")
@pytest.mark.xfail(reason="Unauthorized access to the WFS service")
@pytest.mark.mpl_image_compare(filename='wfs_france.png')
def test_wfs_france():
    ax = plt.axes(projection=ccrs.epsg(2154))
    url = 'https://geodata.inrae.fr/geoserver/wfs'
    typename = 'collectif:t_ser_l93'
    feature = cfeature.WFSFeature(url, typename, edgecolor='red')
    ax.add_feature(feature)
    return ax.figure
