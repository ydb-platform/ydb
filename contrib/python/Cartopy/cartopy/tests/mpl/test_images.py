# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import types

import matplotlib.colors as colors
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
import pytest
import shapely.geometry as sgeom

from cartopy import config
import cartopy.crs as ccrs
import cartopy.io.img_tiles as cimgt
from cartopy.tests.conftest import _HAS_PYKDTREE_OR_SCIPY
import cartopy.tests.test_img_tiles as ctest_tiles


if not _HAS_PYKDTREE_OR_SCIPY:
    pytest.skip('pykdtree or scipy is required', allow_module_level=True)


NATURAL_EARTH_IMG = (config["repo_data_dir"] / 'raster' / 'natural_earth'
                     / '50-natural-earth-1-downsampled.png')
REGIONAL_IMG = (config['repo_data_dir'] / 'raster' / 'sample'
                / 'Miriam.A2012270.2050.2km.jpg')


# We have an exceptionally large tolerance for the web_tiles test.
# The basemap changes on a regular basis (for seasons) and we really only
# care that it is putting images onto the map which are roughly correct.
@pytest.mark.natural_earth
@pytest.mark.network
@pytest.mark.mpl_image_compare(filename='web_tiles.png', tolerance=5.91)
def test_web_tiles():
    extent = [-15, 0.1, 50, 60]
    target_domain = sgeom.Polygon([[extent[0], extent[1]],
                                   [extent[2], extent[1]],
                                   [extent[2], extent[3]],
                                   [extent[0], extent[3]],
                                   [extent[0], extent[1]]])
    map_prj = cimgt.GoogleTiles().crs
    fig = plt.figure()

    ax = fig.add_subplot(2, 2, 1, projection=map_prj)
    gt = cimgt.GoogleTiles()
    gt._image_url = types.MethodType(ctest_tiles.GOOGLE_IMAGE_URL_REPLACEMENT,
                                     gt)
    img, extent, origin = gt.image_for_domain(target_domain, 1)
    ax.imshow(np.array(img), extent=extent, transform=gt.crs,
              interpolation='bilinear', origin=origin)
    ax.coastlines(color='white')

    ax = fig.add_subplot(2, 2, 2, projection=map_prj)
    qt = cimgt.QuadtreeTiles()
    img, extent, origin = qt.image_for_domain(target_domain, 1)
    ax.imshow(np.array(img), extent=extent, transform=qt.crs,
              interpolation='bilinear', origin=origin)
    ax.coastlines(color='white')

    ax = fig.add_subplot(2, 2, 3, projection=map_prj)
    osm = cimgt.OSM()
    img, extent, origin = osm.image_for_domain(target_domain, 1)
    ax.imshow(np.array(img), extent=extent, transform=osm.crs,
              interpolation='bilinear', origin=origin)
    ax.coastlines()

    return fig


@pytest.mark.natural_earth
@pytest.mark.network
@pytest.mark.mpl_image_compare(filename='image_merge.png', tolerance=0.03)
def test_image_merge():
    # tests the basic image merging functionality
    tiles = []
    for i in range(1, 4):
        for j in range(0, 3):
            tiles.append((i, j, 2))

    gt = cimgt.GoogleTiles()
    gt._image_url = types.MethodType(ctest_tiles.GOOGLE_IMAGE_URL_REPLACEMENT,
                                     gt)
    images_to_merge = []
    for tile in tiles:
        img, extent, origin = gt.get_image(tile)
        img = np.array(img)
        x = np.linspace(extent[0], extent[1], img.shape[1], endpoint=False)
        y = np.linspace(extent[2], extent[3], img.shape[0], endpoint=False)
        images_to_merge.append([img, x, y, origin])

    img, extent, origin = cimgt._merge_tiles(images_to_merge)
    ax = plt.axes(projection=gt.crs)
    ax.set_global()
    ax.coastlines()
    ax.imshow(img, origin=origin, extent=extent, alpha=0.5)

    return ax.figure


@pytest.mark.mpl_image_compare(filename='imshow_natural_earth_ortho.png')
def test_imshow():
    source_proj = ccrs.PlateCarree()
    img = plt.imread(NATURAL_EARTH_IMG)
    # Convert the image to a byte array, rather than float, which is the
    # form that JPG images would be loaded with imread.
    img = (img * 255).astype('uint8')
    ax = plt.axes(projection=ccrs.Orthographic())
    ax.imshow(img, transform=source_proj,
              extent=[-180, 180, -90, 90])
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='imshow_regional_projected.png',
                               tolerance=1.97)
def test_imshow_projected():
    source_proj = ccrs.PlateCarree()
    img_extent = (-120.67660000000001, -106.32104523100001,
                  13.2301484511245, 30.766899999999502)
    img = plt.imread(REGIONAL_IMG)
    ax = plt.axes(projection=ccrs.LambertConformal())
    ax.set_extent(img_extent, crs=source_proj)
    ax.coastlines(resolution='50m')
    ax.imshow(img, extent=img_extent, transform=source_proj)
    return ax.figure


def test_imshow_wrapping():
    ax = plt.axes(projection=ccrs.PlateCarree(central_longitude=0.0))
    # Set the extent outside of the current projection domain to ensure
    # it is wrapped back to the (-180, 180) extent of the projection
    ax.imshow(np.random.random((10, 10)), transform=ccrs.PlateCarree(),
              extent=(0, 360, -90, 90))

    assert ax.get_xlim() == (-180, 180)


def test_imshow_arguments():
    """Smoke test for imshow argument passing in the fast-path"""
    ax = plt.axes(projection=ccrs.PlateCarree())
    # Set the regrid_shape parameter to ensure it isn't passed to Axes.imshow()
    # in the fast-path call to super()
    with pytest.warns(UserWarning, match="ignoring regrid_shape"):
        ax.imshow(np.random.random((10, 10)), transform=ccrs.PlateCarree(),
              extent=(-180, 180, -90, 90), regrid_shape=500)


def test_imshow_rgba():
    # tests that the alpha of a RGBA array passed to imshow is set to 0
    # instead of masked
    z = np.full((100, 100), 0.5)
    cmap = plt.get_cmap()
    norm = colors.Normalize(vmin=0, vmax=1)
    z1 = cmap(norm(z))
    plt_crs = ccrs.LambertAzimuthalEqualArea()
    latlon_crs = ccrs.PlateCarree()
    ax = plt.axes(projection=plt_crs)
    ax.set_extent([-30, -20, 60, 70], crs=latlon_crs)
    img = ax.imshow(z1, extent=[-26, -24, 64, 66], transform=latlon_crs)
    assert sum(img.get_array().data[:, 0, 3]) == 0


def test_imshow_rgba_alpha():
    # test that alpha channel from RGBA is not skipped
    dy, dx = (3, 4)

    ax = plt.axes(projection=ccrs.Orthographic(-120, 45))

    # Create RGBA Image with random data and linspace alpha
    RGBA = np.linspace(0, 255 * 31, dx * dy * 4,
                       dtype=np.uint8).reshape((dy, dx, 4))

    alpha = np.array([0, 85, 170, 255])
    RGBA[:, :, 3] = alpha

    img = ax.imshow(RGBA, transform=ccrs.PlateCarree())
    assert np.all(np.unique(img.get_array().data[:, :, 3]) == alpha)


def test_imshow_rgb():
    # tests that the alpha of a RGB array passed to imshow is set to 0
    # instead of masked
    z = np.full((100, 100, 3), 0.5)
    plt_crs = ccrs.LambertAzimuthalEqualArea()
    latlon_crs = ccrs.PlateCarree()
    ax = plt.axes(projection=plt_crs)
    ax.set_extent([-30, -20, 60, 70], crs=latlon_crs)
    img = ax.imshow(z, extent=[-26, -24, 64, 66], transform=latlon_crs)
    assert sum(img.get_array().data[:, 0, 3]) == 0


@pytest.mark.mpl_image_compare(filename='imshow_natural_earth_ortho.png')
def test_stock_img():
    ax = plt.axes(projection=ccrs.Orthographic())
    ax.stock_img()
    return ax.figure


@pytest.mark.mpl_image_compare(filename='imshow_natural_earth_ortho.png')
def test_pil_Image():
    img = Image.open(NATURAL_EARTH_IMG)
    source_proj = ccrs.PlateCarree()
    ax = plt.axes(projection=ccrs.Orthographic())
    ax.imshow(img, transform=source_proj,
              extent=[-180, 180, -90, 90])
    return ax.figure


@pytest.mark.mpl_image_compare(filename='imshow_natural_earth_ortho.png')
def test_background_img():
    ax = plt.axes(projection=ccrs.Orthographic())
    ax.background_img(name='ne_shaded', resolution='low')
    return ax.figure


def test_alpha_2d_warp():
    # tests that both image and alpha arrays (if alpha is 2D) are warped
    plt_crs = ccrs.Geostationary(central_longitude=-155.)
    fig = plt.figure(figsize=(5, 5))
    ax = fig.add_subplot(1, 1, 1, projection=plt_crs)
    latlon_crs = ccrs.PlateCarree()
    coords = [-162., -148., 17.5, 23.]
    ax.set_extent(coords, crs=latlon_crs)
    fake_data = np.zeros([100, 100])
    fake_alphas = np.zeros(fake_data.shape)
    image = ax.imshow(fake_data, extent=coords, transform=latlon_crs,
                      alpha=fake_alphas)
    image_data = image.get_array()
    image_alpha = image.get_alpha()

    assert image_data.shape == image_alpha.shape
