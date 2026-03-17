# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import re

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
from packaging.version import parse as parse_version
import pyproj
import pytest

import cartopy.crs as ccrs
from cartopy.mpl import _MPL_38
from cartopy.tests.conftest import requires_scipy


proj_version = parse_version(pyproj.proj_version_str)

@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='global_contour_wrap.png',
                               style='mpl20', tolerance=2.25)
def test_global_contour_wrap_new_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines()
    x, y = np.meshgrid(np.linspace(0, 360), np.linspace(-90, 90))
    data = np.sin(np.hypot(x, y))
    ax.contour(x, y, data, transform=ccrs.PlateCarree())
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='global_contour_wrap.png',
                               style='mpl20', tolerance=2.25)
def test_global_contour_wrap_no_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines()
    x, y = np.meshgrid(np.linspace(0, 360), np.linspace(-90, 90))
    data = np.sin(np.hypot(x, y))
    ax.contour(x, y, data)
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='global_contourf_wrap.png')
def test_global_contourf_wrap_new_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines()
    x, y = np.meshgrid(np.linspace(0, 360), np.linspace(-90, 90))
    data = np.sin(np.hypot(x, y))
    ax.contourf(x, y, data, transform=ccrs.PlateCarree())
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='global_contourf_wrap.png')
def test_global_contourf_wrap_no_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines()
    x, y = np.meshgrid(np.linspace(0, 360), np.linspace(-90, 90))
    data = np.sin(np.hypot(x, y))
    ax.contourf(x, y, data)
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='global_pcolor_wrap.png')
def test_global_pcolor_wrap_new_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines()
    x, y = np.meshgrid(np.linspace(0, 360), np.linspace(-90, 90))
    data = np.sin(np.hypot(x, y))[:-1, :-1]
    ax.pcolor(x, y, data, transform=ccrs.PlateCarree())
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='global_pcolor_wrap.png')
def test_global_pcolor_wrap_no_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines()
    x, y = np.meshgrid(np.linspace(0, 360), np.linspace(-90, 90))
    data = np.sin(np.hypot(x, y))[:-1, :-1]
    ax.pcolor(x, y, data)
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='global_scatter_wrap.png')
def test_global_scatter_wrap_new_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    # By default the coastline feature will be drawn after patches.
    # By setting zorder we can ensure our scatter points are drawn
    # after the coastlines.
    ax.coastlines(zorder=0)
    x, y = np.meshgrid(np.linspace(0, 360), np.linspace(-90, 90))
    data = np.sin(np.hypot(x, y))
    ax.scatter(x, y, c=data, transform=ccrs.PlateCarree())
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='global_scatter_wrap.png')
def test_global_scatter_wrap_no_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines(zorder=0)
    x, y = np.meshgrid(np.linspace(0, 360), np.linspace(-90, 90))
    data = np.sin(np.hypot(x, y))
    ax.scatter(x, y, c=data)
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='global_hexbin_wrap.png')
def test_global_hexbin_wrap():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines(zorder=2)
    x, y = np.meshgrid(np.arange(-179, 181), np.arange(-90, 91))
    data = np.sin(np.hypot(x, y))
    ax.hexbin(
        x.flatten(),
        y.flatten(),
        C=data.flatten(),
        gridsize=20,
        zorder=1,
    )
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(
    filename='global_hexbin_wrap.png',
    tolerance=0.5)
def test_global_hexbin_wrap_transform():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines(zorder=2)
    x, y = np.meshgrid(np.arange(0, 360), np.arange(-90, 91))
    # wrap values so to match x values from test_global_hexbin_wrap
    x_wrap = np.where(x >= 180, x - 360, x)
    data = np.sin(np.hypot(x_wrap, y))
    ax.hexbin(
        x.flatten(),
        y.flatten(),
        C=data.flatten(),
        gridsize=20,
        zorder=1,
    )
    return ax.figure


@pytest.mark.filterwarnings("ignore:Unable to determine extent")
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='simple_global.png')
def test_simple_global():
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines()
    # produces a global map, despite not having needed to set the limits
    return ax.figure


@pytest.mark.filterwarnings("ignore:Unable to determine extent")
@pytest.mark.natural_earth
@pytest.mark.parametrize('proj', [
    ccrs.Aitoff,
    ccrs.EckertI,
    ccrs.EckertII,
    ccrs.EckertIII,
    ccrs.EckertIV,
    ccrs.EckertV,
    ccrs.EckertVI,
    ccrs.EqualEarth,
    ccrs.Gnomonic,
    ccrs.Hammer,
    pytest.param((ccrs.InterruptedGoodeHomolosine, dict(emphasis='land')),
                 id='InterruptedGoodeHomolosine'),
    ccrs.LambertCylindrical,
    ccrs.LambertZoneII,
    pytest.param(ccrs.Spilhaus,marks=pytest.mark.skipif(
            (proj_version < parse_version("9.6.0")),
            reason="Requires PROJ >= 9.6.0"
        )),
    pytest.param((ccrs.Mercator, dict(min_latitude=-85, max_latitude=85)),
                 id='Mercator'),
    ccrs.Miller,
    ccrs.Mollweide,
    ccrs.NorthPolarStereo,
    ccrs.Orthographic,
    pytest.param((ccrs.OSGB, dict(approx=True)), id='OSGB'),
    ccrs.PlateCarree,
    ccrs.Robinson,
    pytest.param((ccrs.RotatedPole,
                  dict(pole_latitude=45, pole_longitude=180)),
                 id='RotatedPole'),
    ccrs.Stereographic,
    ccrs.SouthPolarStereo,
    pytest.param((ccrs.TransverseMercator, dict(approx=True)),
                 id='TransverseMercator'),
    pytest.param(
        (ccrs.ObliqueMercator, dict(azimuth=0.)), id='ObliqueMercator_default'
    ),
    pytest.param(
        (ccrs.ObliqueMercator, dict(azimuth=90., central_latitude=-22)),
        id='ObliqueMercator_rotated',
    ),
])
@pytest.mark.mpl_image_compare(style='mpl20')
def test_global_map(proj):
    if isinstance(proj, tuple):
        proj, kwargs = proj
    else:
        kwargs = {}
    proj = proj(**kwargs)

    fig = plt.figure(figsize=(2, 2))
    ax = fig.add_subplot(projection=proj)
    ax.set_global()
    ax.coastlines(resolution="110m")

    ax.plot(-0.08, 51.53, 'o', transform=ccrs.PlateCarree())
    ax.plot([-0.08, 132], [51.53, 43.17], color='red',
            transform=ccrs.PlateCarree())
    ax.plot([-0.08, 132], [51.53, 43.17], color='blue',
            transform=ccrs.Geodetic())

    return fig


def test_cursor_values():
    ax = plt.axes(projection=ccrs.NorthPolarStereo())
    x, y = np.array([-969100., -4457000.])
    r = ax.format_coord(x, y)
    assert (r.encode('ascii', 'ignore') ==
            b'-9.691e+05, -4.457e+06 (50.716617N, 12.267069W)')

    ax = plt.axes(projection=ccrs.PlateCarree())
    x, y = np.array([-181.5, 50.])
    r = ax.format_coord(x, y)
    assert (r.encode('ascii', 'ignore') ==
            b'-181.5, 50 (50.000000N, 178.500000E)')

    ax = plt.axes(projection=ccrs.Robinson())
    x, y = np.array([16060595.2, 2363093.4])
    r = ax.format_coord(x, y)
    assert re.search(b'1.606e\\+07, 2.363e\\+06 '
                     b'\\(22.09[0-9]{4}N, 173.70[0-9]{4}E\\)',
                     r.encode('ascii', 'ignore'))


SKIP_PRE_MPL38 = pytest.mark.skipif(not _MPL_38, reason='mpl < 3.8')
PARAMETRIZE_PCOLORMESH_WRAP = pytest.mark.parametrize(
    'mesh_data_kind',
    [
        'standard',
        pytest.param('rgb', marks=SKIP_PRE_MPL38),
        pytest.param('rgba', marks=SKIP_PRE_MPL38),
    ],
    ids=['standard', 'rgb', 'rgba'],
)


def _to_rgb(data, mesh_data_kind):
    """
    Helper function to convert array to RGB(A) where required
    """
    if mesh_data_kind in ('rgb', 'rgba'):
        cmap = plt.get_cmap()
        norm = mcolors.Normalize()
        new_data = cmap(norm(data))
        if mesh_data_kind == 'rgb':
            new_data = new_data[..., 0:3]
            if np.ma.is_masked(data):
                # Use data's mask as an alpha channel
                mask = np.ma.getmaskarray(data)
                mask = np.broadcast_to(
                    mask[..., np.newaxis], new_data.shape).copy()
                new_data = np.ma.array(new_data, mask=mask)

        return new_data

    return data


@PARAMETRIZE_PCOLORMESH_WRAP
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_global_wrap1.png',
                               tolerance=1.27)
def test_pcolormesh_global_with_wrap1(mesh_data_kind):
    # make up some realistic data with bounds (such as data from the UM)
    nx, ny = 36, 18
    xbnds = np.linspace(0, 360, nx, endpoint=True)
    ybnds = np.linspace(-90, 90, ny, endpoint=True)

    x, y = np.meshgrid(xbnds, ybnds)
    data = np.exp(np.sin(np.deg2rad(x)) + np.cos(np.deg2rad(y)))
    data = data[:-1, :-1]
    fig = plt.figure()

    data = _to_rgb(data, mesh_data_kind)

    ax = fig.add_subplot(2, 1, 1, projection=ccrs.PlateCarree())
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(), snap=False)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    ax = fig.add_subplot(2, 1, 2, projection=ccrs.PlateCarree(180))
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(), snap=False)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    return fig


@PARAMETRIZE_PCOLORMESH_WRAP
def test_pcolormesh_get_array_with_mask(mesh_data_kind):
    # make up some realistic data with bounds (such as data from the UM)
    nx, ny = 36, 18
    xbnds = np.linspace(0, 360, nx, endpoint=True)
    ybnds = np.linspace(-90, 90, ny, endpoint=True)

    x, y = np.meshgrid(xbnds, ybnds)
    data = np.exp(np.sin(np.deg2rad(x)) + np.cos(np.deg2rad(y)))
    data[5, :] = np.nan  # Check that missing data is handled - GH#2208
    data = data[:-1, :-1]
    data = _to_rgb(data, mesh_data_kind)

    fig = plt.figure()

    ax = fig.add_subplot(2, 1, 1, projection=ccrs.PlateCarree())
    c = ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree())
    assert c._wrapped_collection_fix is not None, \
        'No pcolormesh wrapping was done when it should have been.'

    result = c.get_array()
    np.testing.assert_array_equal(np.ma.getmask(result), np.isnan(data))
    np.testing.assert_array_equal(
        data, result,
        err_msg='Data supplied does not match data retrieved in wrapped case')

    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    # Case without wrapping
    nx, ny = 36, 18
    xbnds = np.linspace(-60, 60, nx, endpoint=True)
    ybnds = np.linspace(-80, 80, ny, endpoint=True)

    x, y = np.meshgrid(xbnds, ybnds)
    data = np.exp(np.sin(np.deg2rad(x)) + np.cos(np.deg2rad(y)))
    data[5, :] = np.nan
    data2 = data[:-1, :-1]
    data2 = _to_rgb(data2, mesh_data_kind)

    ax = fig.add_subplot(2, 1, 2, projection=ccrs.PlateCarree())
    c = ax.pcolormesh(xbnds, ybnds, data2, transform=ccrs.PlateCarree())
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    assert getattr(c, "_wrapped_collection_fix", None) is None, \
        'pcolormesh wrapping was done when it should not have been.'

    result = c.get_array()

    expected = data2
    if not _MPL_38:
        expected = expected.ravel()

    np.testing.assert_array_equal(np.ma.getmask(result), np.isnan(expected))
    np.testing.assert_array_equal(
        expected, result,
        'Data supplied does not match data retrieved in unwrapped case')


@PARAMETRIZE_PCOLORMESH_WRAP
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_global_wrap2.png',
                               tolerance=1.87)
def test_pcolormesh_global_with_wrap2(mesh_data_kind):
    # make up some realistic data with bounds (such as data from the UM)
    nx, ny = 36, 18
    xbnds, xstep = np.linspace(0, 360, nx - 1, retstep=True, endpoint=True)
    ybnds, ystep = np.linspace(-90, 90, ny - 1, retstep=True, endpoint=True)
    xbnds -= xstep / 2
    ybnds -= ystep / 2
    xbnds = np.append(xbnds, xbnds[-1] + xstep)
    ybnds = np.append(ybnds, ybnds[-1] + ystep)

    x, y = np.meshgrid(xbnds, ybnds)
    data = np.exp(np.sin(np.deg2rad(x)) + np.cos(np.deg2rad(y)))
    data = data[:-1, :-1]
    fig = plt.figure()

    data = _to_rgb(data, mesh_data_kind)

    ax = fig.add_subplot(2, 1, 1, projection=ccrs.PlateCarree())
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(), snap=False)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    ax = fig.add_subplot(2, 1, 2, projection=ccrs.PlateCarree(180))
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(), snap=False)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    return fig


@PARAMETRIZE_PCOLORMESH_WRAP
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_global_wrap3.png',
                               tolerance=1.42)
def test_pcolormesh_global_with_wrap3(mesh_data_kind):
    nx, ny = 33, 17
    xbnds = np.linspace(-1.875, 358.125, nx, endpoint=True)
    ybnds = np.linspace(91.25, -91.25, ny, endpoint=True)
    xbnds, ybnds = np.meshgrid(xbnds, ybnds)

    data = np.exp(np.sin(np.deg2rad(xbnds)) + np.cos(np.deg2rad(ybnds)))

    # this step is not necessary, but makes the plot even harder to do (i.e.
    # it really puts cartopy through its paces)
    ybnds = np.append(ybnds, ybnds[:, 1:2], axis=1)
    xbnds = np.append(xbnds, xbnds[:, 1:2] + 360, axis=1)
    data = np.ma.concatenate([data, data[:, 0:1]], axis=1)

    data = data[:-1, :-1]
    data = np.ma.masked_greater(data, 2.6)
    fig = plt.figure()

    data = _to_rgb(data, mesh_data_kind)

    ax = fig.add_subplot(3, 1, 1, projection=ccrs.PlateCarree(-45))
    c = ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(),
                      snap=False)
    assert c._wrapped_collection_fix is not None, \
        'No pcolormesh wrapping was done when it should have been.'

    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    ax = fig.add_subplot(3, 1, 2, projection=ccrs.PlateCarree(-1.87499952))
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(), snap=False)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    ax = fig.add_subplot(3, 1, 3, projection=ccrs.Robinson(-2))
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(), snap=False)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    return fig


@PARAMETRIZE_PCOLORMESH_WRAP
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_global_wrap3.png',
                               tolerance=1.42)
def test_pcolormesh_set_array_with_mask(mesh_data_kind):
    """Testing that set_array works with masked arrays properly."""
    nx, ny = 33, 17
    xbnds = np.linspace(-1.875, 358.125, nx, endpoint=True)
    ybnds = np.linspace(91.25, -91.25, ny, endpoint=True)
    xbnds, ybnds = np.meshgrid(xbnds, ybnds)

    data = np.exp(np.sin(np.deg2rad(xbnds)) + np.cos(np.deg2rad(ybnds)))

    # this step is not necessary, but makes the plot even harder to do (i.e.
    # it really puts cartopy through its paces)
    ybnds = np.append(ybnds, ybnds[:, 1:2], axis=1)
    xbnds = np.append(xbnds, xbnds[:, 1:2] + 360, axis=1)
    data = np.ma.concatenate([data, data[:, 0:1]], axis=1)

    data = data[:-1, :-1]
    data = np.ma.masked_greater(data, 2.6)
    norm = plt.Normalize(np.min(data), np.max(data))
    bad_data = np.ones(data.shape)
    # Start with the opposite mask and then swap back in the set_array call
    bad_data_mask = np.ma.array(bad_data, mask=~data.mask)
    fig = plt.figure()

    data = _to_rgb(data, mesh_data_kind)
    bad_data = _to_rgb(bad_data, mesh_data_kind)
    bad_data_mask = _to_rgb(bad_data_mask, mesh_data_kind)

    ax = fig.add_subplot(3, 1, 1, projection=ccrs.PlateCarree(-45))
    c = ax.pcolormesh(xbnds, ybnds, bad_data,
                      norm=norm, transform=ccrs.PlateCarree(), snap=False)

    c.set_array(data)
    assert c._wrapped_collection_fix is not None, \
        'No pcolormesh wrapping was done when it should have been.'

    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    ax = fig.add_subplot(3, 1, 2, projection=ccrs.PlateCarree(-1.87499952))
    c2 = ax.pcolormesh(xbnds, ybnds, bad_data_mask,
                       norm=norm, transform=ccrs.PlateCarree(), snap=False)
    if mesh_data_kind == 'standard':
        c2.set_array(data.ravel())
    else:
        c2.set_array(data)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    ax = fig.add_subplot(3, 1, 3, projection=ccrs.Robinson(-2))
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(), snap=False)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    return fig


def test_pcolormesh_set_array_nowrap():
    # Roundtrip check that set_array works with the correct shaped arrays
    nx, ny = 36, 18
    xbnds = np.linspace(-60, 60, nx, endpoint=True)
    ybnds = np.linspace(-80, 80, ny, endpoint=True)
    xbnds, ybnds = np.meshgrid(xbnds, ybnds)

    rng = np.random.default_rng()
    data = rng.random((ny - 1, nx - 1))

    ax = plt.figure().add_subplot(projection=ccrs.PlateCarree())
    mesh = ax.pcolormesh(xbnds, ybnds, data)
    assert not hasattr(mesh, '_wrapped_collection_fix')

    expected = data
    if not _MPL_38:
        expected = expected.ravel()
    np.testing.assert_array_equal(mesh.get_array(), expected)

    # For backwards compatibility, check we can set a 1D array
    data = rng.random((nx - 1) * (ny - 1))
    mesh.set_array(data)
    np.testing.assert_array_equal(
        mesh.get_array(), data.reshape(ny - 1, nx - 1))

    # Check that we can set a 2D array even if previous was flat
    data = rng.random((ny - 1, nx - 1))
    mesh.set_array(data)
    np.testing.assert_array_equal(mesh.get_array(), data)


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_global_wrap3.png',
                               tolerance=1.42)
def test_pcolormesh_set_clim_with_mask():
    """Testing that set_clim works with masked arrays properly."""
    nx, ny = 33, 17
    xbnds = np.linspace(-1.875, 358.125, nx, endpoint=True)
    ybnds = np.linspace(91.25, -91.25, ny, endpoint=True)
    xbnds, ybnds = np.meshgrid(xbnds, ybnds)

    data = np.exp(np.sin(np.deg2rad(xbnds)) + np.cos(np.deg2rad(ybnds)))

    # this step is not necessary, but makes the plot even harder to do (i.e.
    # it really puts cartopy through its paces)
    ybnds = np.append(ybnds, ybnds[:, 1:2], axis=1)
    xbnds = np.append(xbnds, xbnds[:, 1:2] + 360, axis=1)
    data = np.ma.concatenate([data, data[:, 0:1]], axis=1)

    data = data[:-1, :-1]
    data = np.ma.masked_greater(data, 2.6)

    bad_initial_norm = plt.Normalize(-100, 100)
    fig = plt.figure()

    ax = fig.add_subplot(3, 1, 1, projection=ccrs.PlateCarree(-45))
    c = ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(),
                      norm=bad_initial_norm, snap=False)
    assert c._wrapped_collection_fix is not None, \
        'No pcolormesh wrapping was done when it should have been.'

    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    ax = fig.add_subplot(3, 1, 2, projection=ccrs.PlateCarree(-1.87499952))
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(), snap=False)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    ax = fig.add_subplot(3, 1, 3, projection=ccrs.Robinson(-2))
    ax.pcolormesh(xbnds, ybnds, data, transform=ccrs.PlateCarree(), snap=False)
    ax.coastlines()
    ax.set_global()  # make sure everything is visible

    # Fix clims on c so that test passes
    c.set_clim(data.min(), data.max())

    return fig


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_limited_area_wrap.png',
                               tolerance=1.83)
def test_pcolormesh_limited_area_wrap():
    # make up some realistic data with bounds (such as data from the UM's North
    # Atlantic Europe model)
    nx, ny = 22, 36
    xbnds = np.linspace(311.91998291, 391.11999512, nx, endpoint=True)
    ybnds = np.linspace(-23.59000015, 24.81000137, ny, endpoint=True)
    x, y = np.meshgrid(xbnds, ybnds)
    data = ((np.sin(np.deg2rad(x))) / 10. + np.exp(np.cos(np.deg2rad(y))))
    data = data[:-1, :-1]

    rp = ccrs.RotatedPole(pole_longitude=177.5, pole_latitude=37.5)

    fig = plt.figure(figsize=(10, 6))

    ax = fig.add_subplot(2, 2, 1, projection=ccrs.PlateCarree())
    ax.pcolormesh(xbnds, ybnds, data, transform=rp, cmap='Spectral',
                  snap=False)
    ax.coastlines()

    ax = fig.add_subplot(2, 2, 2, projection=ccrs.PlateCarree(180))
    ax.pcolormesh(xbnds, ybnds, data, transform=rp, cmap='Spectral',
                  snap=False)
    ax.coastlines()
    ax.set_global()

    # draw the same plot, only more zoomed in, and using the 2d versions
    # of the coordinates (just to test that 1d and 2d are both suitably
    # being fixed)
    ax = fig.add_subplot(2, 2, 3, projection=ccrs.PlateCarree())
    ax.pcolormesh(x, y, data, transform=rp, cmap='Spectral', snap=False)
    ax.coastlines()
    ax.set_extent([-70, 0, 0, 80])

    ax = fig.add_subplot(2, 2, 4, projection=rp)
    ax.pcolormesh(xbnds, ybnds, data, transform=rp, cmap='Spectral',
                  snap=False)
    ax.coastlines()

    return fig


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_single_column_wrap.png')
def test_pcolormesh_single_column_wrap():
    # Check a wrapped mesh like test_pcolormesh_limited_area_wrap, but only use
    # a single column, which could break depending on how wrapping is
    # determined.
    ny = 36
    xbnds = np.array([360.9485619, 364.71999105])
    ybnds = np.linspace(-23.59000015, 24.81000137, ny, endpoint=True)
    x, y = np.meshgrid(xbnds, ybnds)
    data = ((np.sin(np.deg2rad(x))) / 10. + np.exp(np.cos(np.deg2rad(y))))
    data = data[:-1, :-1]

    rp = ccrs.RotatedPole(pole_longitude=177.5, pole_latitude=37.5)

    fig = plt.figure(figsize=(10, 6))

    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree(180))
    # TODO: Remove snap when updating this image
    ax.pcolormesh(xbnds, ybnds, data, transform=rp, cmap='Spectral',
                  snap=False)
    ax.coastlines()
    ax.set_global()

    return fig


def test_pcolormesh_wrap_gouraud_shading_failing_mask_creation():
    x_range = np.linspace(-180, 180, 50)
    y_range = np.linspace(90, -90, 50)
    x, y = np.meshgrid(x_range, y_range)
    data = ((np.sin(np.deg2rad(x))) / 10. + np.exp(np.cos(np.deg2rad(y))))

    fig = plt.figure(figsize=(10, 6))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.Mercator())
    ax.pcolormesh(x, y, data, transform=ccrs.PlateCarree(), shading='gouraud')


def test_pcolormesh_diagonal_wrap():
    # Check that a cell with the top edge on one side of the domain
    # and the bottom edge on the other gets wrapped properly
    xs = [[160, 170], [190, 200]]
    ys = [[-10, -10], [10, 10]]
    zs = [[0]]

    ax = plt.axes(projection=ccrs.PlateCarree())
    mesh = ax.pcolormesh(xs, ys, zs)

    # And that the wrapped_collection is added
    assert hasattr(mesh, "_wrapped_collection_fix")


def test_pcolormesh_nan_wrap():
    # Check that data with nan's as input still creates
    # the proper number of pcolor cells and those aren't
    # masked in the process.
    xs, ys = np.meshgrid([120, 160, 200], [-30, 0, 30])
    data = np.ones((2, 2)) * np.nan

    ax = plt.axes(projection=ccrs.PlateCarree())
    mesh = ax.pcolormesh(xs, ys, data)
    pcolor = getattr(mesh, "_wrapped_collection_fix")
    if not _MPL_38:
        assert len(pcolor.get_paths()) == 2
    else:
        assert not pcolor.get_paths()

    # Check that we can populate the pcolor with some data.
    mesh.set_array(np.ones((2, 2)))
    assert len(pcolor.get_paths()) == 2


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_goode_wrap.png')
def test_pcolormesh_goode_wrap():
    # global data on an Interrupted Goode Homolosine projection
    # shouldn't spill outside projection boundary
    x = np.linspace(0, 360, 73)
    y = np.linspace(-87.5, 87.5, 36)
    X, Y = np.meshgrid(*[np.deg2rad(c) for c in (x, y)])
    Z = np.cos(Y) + 0.375 * np.sin(2. * X)
    Z = Z[:-1, :-1]
    ax = plt.axes(projection=ccrs.InterruptedGoodeHomolosine(emphasis='land'))
    ax.coastlines()
    # TODO: Remove snap when updating this image
    ax.pcolormesh(x, y, Z, transform=ccrs.PlateCarree(), snap=False)
    return ax.figure


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_mercator_wrap.png')
def test_pcolormesh_mercator_wrap():
    x = np.linspace(0, 360, 73)
    y = np.linspace(-87.5, 87.5, 36)
    X, Y = np.meshgrid(*[np.deg2rad(c) for c in (x, y)])
    Z = np.cos(Y) + 0.375 * np.sin(2. * X)
    Z = Z[:-1, :-1]
    ax = plt.axes(projection=ccrs.Mercator())
    ax.coastlines()
    ax.pcolormesh(x, y, Z, transform=ccrs.PlateCarree(), snap=False)
    return ax.figure


@PARAMETRIZE_PCOLORMESH_WRAP
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='pcolormesh_mercator_wrap.png')
def test_pcolormesh_wrap_set_array(mesh_data_kind):
    x = np.linspace(0, 360, 73)
    y = np.linspace(-87.5, 87.5, 36)
    X, Y = np.meshgrid(*[np.deg2rad(c) for c in (x, y)])
    Z = np.cos(Y) + 0.375 * np.sin(2. * X)
    Z = Z[:-1, :-1]

    Z = _to_rgb(Z, mesh_data_kind)

    ax = plt.axes(projection=ccrs.Mercator())
    norm = plt.Normalize(np.min(Z), np.max(Z))
    ax.coastlines()
    # Start off with bad data
    coll = ax.pcolormesh(x, y, np.ones(Z.shape), norm=norm,
                         transform=ccrs.PlateCarree(), snap=False)
    # Now update the plot with the set_array method
    coll.set_array(Z)
    return ax.figure


@pytest.mark.parametrize('shading, input_size, expected', [
    pytest.param('auto', 3, 4, id='auto same size'),
    pytest.param('auto', 4, 4, id='auto input larger'),
    pytest.param('nearest', 3, 4, id='nearest same size'),
    pytest.param('nearest', 4, 4, id='nearest input larger'),
    pytest.param('flat', 4, 4, id='flat input larger'),
    pytest.param('gouraud', 3, 3, id='gouraud same size')
])
def test_pcolormesh_shading(shading, input_size, expected):
    # Testing that the coordinates are all broadcast as expected with
    # the various shading options
    # The data shape is (3, 3) and we are changing the input shape
    # based upon that
    ax = plt.axes(projection=ccrs.PlateCarree())

    x = np.arange(input_size)
    y = np.arange(input_size)
    d = np.zeros((3, 3))

    coll = ax.pcolormesh(x, y, d, shading=shading)
    assert coll.get_coordinates().shape == (expected, expected, 2)


def test__wrap_args_default_shading():
    # Passing shading=None should give the same as not passing the shading parameter.
    x = np.linspace(0, 360, 12)
    y = np.linspace(0, 90, 5)
    z = np.zeros((12, 5))

    ax = plt.subplot(projection=ccrs.Orthographic())
    args_ref, kwargs_ref = ax._wrap_args(x, y, z, transform=ccrs.PlateCarree())
    args_test, kwargs_test = ax._wrap_args(
        x, y, z, transform=ccrs.PlateCarree(), shading=None)

    for array_ref, array_test in zip(args_ref, args_test):
        np.testing.assert_allclose(array_ref, array_test)

    assert kwargs_ref == kwargs_test


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='quiver_plate_carree.png')
def test_quiver_plate_carree():
    x = np.arange(-60, 42.5, 2.5)
    y = np.arange(30, 72.5, 2.5)
    x2d, y2d = np.meshgrid(x, y)
    u = np.cos(np.deg2rad(y2d))
    v = np.cos(2. * np.deg2rad(x2d))
    mag = np.hypot(u, v)
    plot_extent = [-60, 40, 30, 70]
    fig = plt.figure(figsize=(6, 6))
    # plot on native projection
    ax = fig.add_subplot(2, 1, 1, projection=ccrs.PlateCarree())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines(resolution="110m")
    ax.quiver(x, y, u, v, mag)
    # plot on a different projection
    ax = fig.add_subplot(2, 1, 2, projection=ccrs.NorthPolarStereo())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines()
    ax.quiver(x, y, u, v, mag, transform=ccrs.PlateCarree())
    return fig


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='quiver_rotated_pole.png')
def test_quiver_rotated_pole():
    nx, ny = 22, 36
    x = np.linspace(311.91998291, 391.11999512, nx, endpoint=True)
    y = np.linspace(-23.59000015, 24.81000137, ny, endpoint=True)
    x2d, y2d = np.meshgrid(x, y)
    u = np.cos(np.deg2rad(y2d))
    v = -2. * np.cos(2. * np.deg2rad(y2d)) * np.sin(np.deg2rad(x2d))
    mag = np.hypot(u, v)
    rp = ccrs.RotatedPole(pole_longitude=177.5, pole_latitude=37.5)
    plot_extent = [x[0], x[-1], y[0], y[-1]]
    # plot on native projection
    fig = plt.figure(figsize=(6, 6))
    ax = fig.add_subplot(2, 1, 1, projection=rp)
    ax.set_extent(plot_extent, crs=rp)
    ax.coastlines()
    ax.quiver(x, y, u, v, mag)
    # plot on different projection
    ax = fig.add_subplot(2, 1, 2, projection=ccrs.PlateCarree())
    ax.set_extent(plot_extent, crs=rp)
    ax.coastlines()
    ax.quiver(x, y, u, v, mag, transform=rp)
    return fig


@requires_scipy
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='quiver_regrid.png')
def test_quiver_regrid():
    x = np.arange(-60, 42.5, 2.5)
    y = np.arange(30, 72.5, 2.5)
    x2d, y2d = np.meshgrid(x, y)
    u = np.cos(np.deg2rad(y2d))
    v = np.cos(2. * np.deg2rad(x2d))
    mag = np.hypot(u, v)
    plot_extent = [-60, 40, 30, 70]
    fig = plt.figure(figsize=(6, 3))
    ax = fig.add_subplot(projection=ccrs.NorthPolarStereo())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines()
    ax.quiver(x, y, u, v, mag, transform=ccrs.PlateCarree(),
              regrid_shape=30)
    return fig


@requires_scipy
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='quiver_regrid_with_extent.png',
                               tolerance=0.54)
def test_quiver_regrid_with_extent():
    x = np.arange(-60, 42.5, 2.5)
    y = np.arange(30, 72.5, 2.5)
    x2d, y2d = np.meshgrid(x, y)
    u = np.cos(np.deg2rad(y2d))
    v = np.cos(2. * np.deg2rad(x2d))
    mag = np.hypot(u, v)
    plot_extent = [-60, 40, 30, 70]
    target_extent = [-3e6, 2e6, -6e6, -2.5e6]
    fig = plt.figure(figsize=(6, 3))
    ax = fig.add_subplot(projection=ccrs.NorthPolarStereo())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines()
    ax.quiver(x, y, u, v, mag, transform=ccrs.PlateCarree(),
              regrid_shape=10, target_extent=target_extent)
    return fig


@requires_scipy
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='barbs_plate_carree.png')
def test_barbs():
    x = np.arange(-60, 45, 5)
    y = np.arange(30, 75, 5)
    x2d, y2d = np.meshgrid(x, y)
    u = 40 * np.cos(np.deg2rad(y2d))
    v = 40 * np.cos(2. * np.deg2rad(x2d))
    plot_extent = [-60, 40, 30, 70]
    fig = plt.figure(figsize=(6, 6))
    # plot on native projection
    ax = fig.add_subplot(2, 1, 1, projection=ccrs.PlateCarree())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines(resolution="110m")
    ax.barbs(x, y, u, v, length=4, linewidth=.25)
    # plot on a different projection
    ax = fig.add_subplot(2, 1, 2, projection=ccrs.NorthPolarStereo())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines(resolution="110m")
    ax.barbs(x, y, u, v, transform=ccrs.PlateCarree(), length=4, linewidth=.25)
    return fig


@requires_scipy
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='barbs_regrid.png')
def test_barbs_regrid():
    x = np.arange(-60, 42.5, 2.5)
    y = np.arange(30, 72.5, 2.5)
    x2d, y2d = np.meshgrid(x, y)
    u = 40 * np.cos(np.deg2rad(y2d))
    v = 40 * np.cos(2. * np.deg2rad(x2d))
    mag = np.hypot(u, v)
    plot_extent = [-60, 40, 30, 70]
    fig = plt.figure(figsize=(6, 3))
    ax = fig.add_subplot(projection=ccrs.NorthPolarStereo())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines()
    ax.barbs(x, y, u, v, mag, transform=ccrs.PlateCarree(),
             length=4, linewidth=.4, regrid_shape=20)
    return fig


@requires_scipy
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='barbs_regrid_with_extent.png',
                               tolerance=0.54)
def test_barbs_regrid_with_extent():
    x = np.arange(-60, 42.5, 2.5)
    y = np.arange(30, 72.5, 2.5)
    x2d, y2d = np.meshgrid(x, y)
    u = 40 * np.cos(np.deg2rad(y2d))
    v = 40 * np.cos(2. * np.deg2rad(x2d))
    mag = np.hypot(u, v)
    plot_extent = [-60, 40, 30, 70]
    target_extent = [-3e6, 2e6, -6e6, -2.5e6]
    fig = plt.figure(figsize=(6, 3))
    ax = fig.add_subplot(projection=ccrs.NorthPolarStereo())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines()
    ax.barbs(x, y, u, v, mag, transform=ccrs.PlateCarree(),
             length=4, linewidth=.25, regrid_shape=10,
             target_extent=target_extent)
    return fig


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='barbs_1d.png')
def test_barbs_1d():
    x = np.array([20., 30., -17., 15.])
    y = np.array([-1., 35., 11., 40.])
    u = np.array([23., -18., 2., -11.])
    v = np.array([5., -4., 19., 11.])
    plot_extent = [-21, 40, -5, 45]
    fig = plt.figure(figsize=(6, 5))
    ax = fig.add_subplot(projection=ccrs.PlateCarree())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines(resolution="110m")
    ax.barbs(x, y, u, v, transform=ccrs.PlateCarree(),
             length=8, linewidth=1, color='#7f7f7f')
    return fig


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='barbs_1d_transformed.png')
def test_barbs_1d_transformed():
    x = np.array([20., 30., -17., 15.])
    y = np.array([-1., 35., 11., 40.])
    u = np.array([23., -18., 2., -11.])
    v = np.array([5., -4., 19., 11.])
    plot_extent = [-20, 31, -5, 45]
    fig = plt.figure(figsize=(6, 5))
    ax = fig.add_subplot(projection=ccrs.NorthPolarStereo())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines()
    ax.barbs(x, y, u, v, transform=ccrs.PlateCarree(),
             length=8, linewidth=1, color='#7f7f7f')
    return fig


@requires_scipy
@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare(filename='streamplot.png', style='mpl20')
def test_streamplot():
    x = np.arange(-60, 42.5, 2.5)
    y = np.arange(30, 72.5, 2.5)
    x2d, y2d = np.meshgrid(x, y)
    u = np.cos(np.deg2rad(y2d))
    v = np.cos(2. * np.deg2rad(x2d))
    mag = np.hypot(u, v)
    plot_extent = [-60, 40, 30, 70]
    fig = plt.figure(figsize=(6, 3))
    ax = fig.add_subplot(projection=ccrs.NorthPolarStereo())
    ax.set_extent(plot_extent, crs=ccrs.PlateCarree())
    ax.coastlines()
    ax.streamplot(x, y, u, v, transform=ccrs.PlateCarree(),
                  density=(1.5, 2), color=mag, linewidth=2 * mag)
    return fig


@pytest.mark.natural_earth
@pytest.mark.mpl_image_compare()
def test_annotate():
    """ test a variety of annotate options on multiple projections

    Annotate defaults to coords passed as if they're in map projection space.
    `transform` or `xycoords` & `textcoords` control the marker and text offset
    through shared or independent projections or coordinates.
    `transform` is a cartopy kwarg so expects a CRS,
    `xycoords` and `textcoords` accept CRS or matplotlib args.

    The various annotations below test a variety of the different combinations.
    """
    # use IGH to test annotations cross projection splits and map boundaries
    map_projection = ccrs.InterruptedGoodeHomolosine()

    fig = plt.figure(figsize=(10, 5))
    ax = fig.add_subplot(1, 1, 1, projection=map_projection)
    ax.set_global()
    ax.coastlines()
    arrowprops = {'facecolor': 'red',
                  'arrowstyle': "-|>",
                  'connectionstyle': "arc3,rad=-0.2",
                  }
    platecarree = ccrs.PlateCarree()
    mpltransform = platecarree._as_mpl_transform(ax)

    # Add annotation with xycoords as mpltransform as suggested here
    # https://stackoverflow.com/questions/25416600/why-the-annotate-worked-unexpected-here-in-cartopy/25421922#25421922
    ax.annotate('mpl xycoords', (-45, 43), xycoords=mpltransform,
                size=5)

    # Add annotation with xycoords as projection
    ax.annotate('crs xycoords', (-75, 13), xycoords=platecarree,
                size=5)

    # set up coordinates in map projection space
    map_coords = map_projection.transform_point(-175, -35, platecarree)
    # Don't specify any args, default xycoords='data', transform=map projection
    ax.annotate('default crs', map_coords, size=5)

    # data in map projection using default transform, with
    # text positioned in platecarree transform
    ax.annotate('mixed crs transforms', map_coords, xycoords='data',
                xytext=(-175, -55),
                textcoords=platecarree,
                size=5,
                arrowprops=arrowprops,
                )

    # Add annotation with point and text via transform
    ax.annotate('crs transform', (-75, -20), xytext=(0, -55),
                transform=platecarree,
                arrowprops=arrowprops,
                )

    # Add annotation with point via transform and text non transformed
    ax.annotate('offset textcoords', (-149.8, 61.22), transform=platecarree,
                xytext=(-35, 10), textcoords='offset points',
                size=5,
                ha='right',
                arrowprops=arrowprops,
                )

    return fig


def test_inset_axes():
    fig, ax = plt.subplots()
    ax.inset_axes([0.75, 0.75, 0.25, 0.25], projection=ccrs.PlateCarree())
    fig.draw_without_rendering()
