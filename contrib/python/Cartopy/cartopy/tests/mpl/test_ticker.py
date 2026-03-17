# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from unittest.mock import Mock

import matplotlib.pyplot as plt
import numpy as np
import pytest

import cartopy.crs as ccrs
from cartopy.mpl.geoaxes import GeoAxes
from cartopy.mpl.ticker import (
    LatitudeFormatter,
    LatitudeLocator,
    LongitudeFormatter,
    LongitudeLocator,
)


ONE_MIN = 1 / 60.
ONE_SEC = 1 / 3600.


@pytest.mark.parametrize('cls', [LatitudeFormatter, LongitudeFormatter])
def test_formatter_bad_projection(cls):
    formatter = cls()
    match = r'This formatter cannot be used with non-rectangular projections\.'
    with pytest.raises(TypeError, match=match):
        formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=ccrs.Orthographic())))


def test_LatitudeFormatter():
    formatter = LatitudeFormatter()
    p = ccrs.PlateCarree()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-90, -60, -30, 0, 30, 60, 90]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['90°S', '60°S', '30°S', '0°', '30°N', '60°N', '90°N']
    assert result == expected


def test_LatitudeFormatter_direction_label():
    formatter = LatitudeFormatter(direction_label=False)
    p = ccrs.PlateCarree()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-90, -60, -30, 0, 30, 60, 90]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['-90°', '-60°', '-30°', '0°', '30°', '60°', '90°']
    assert result == expected


def test_LatitudeFormatter_degree_symbol():
    formatter = LatitudeFormatter(degree_symbol='')
    p = ccrs.PlateCarree()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-90, -60, -30, 0, 30, 60, 90]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['90S', '60S', '30S', '0', '30N', '60N', '90N']
    assert result == expected


def test_LatitudeFormatter_number_format():
    formatter = LatitudeFormatter(number_format='.2f', dms=False)
    p = ccrs.PlateCarree()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-90, -60, -30, 0, 30, 60, 90]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['90.00°S', '60.00°S', '30.00°S', '0.00°',
                '30.00°N', '60.00°N', '90.00°N']
    assert result == expected


def test_LatitudeFormatter_mercator():
    formatter = LatitudeFormatter()
    p = ccrs.Mercator()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-15496570.739707904, -8362698.548496634,
                  -3482189.085407435, 0.0, 3482189.085407435,
                  8362698.548496634, 15496570.739707898]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['80°S', '60°S', '30°S', '0°', '30°N', '60°N', '80°N']
    assert result == expected


def test_LatitudeFormatter_small_numbers():
    formatter = LatitudeFormatter(number_format='.7f', dms=False)
    p = ccrs.PlateCarree()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [40.1275150, 40.1275152, 40.1275154]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['40.1275150°N', '40.1275152°N', '40.1275154°N']
    assert result == expected


def test_LongitudeFormatter_direction_label():
    formatter = LongitudeFormatter(direction_label=False,
                                   dateline_direction_label=True,
                                   zero_direction_label=True)
    p = ccrs.PlateCarree()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-180, -120, -60, 0, 60, 120, 180]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['-180°', '-120°', '-60°', '0°', '60°', '120°', '180°']
    assert result == expected


@pytest.mark.parametrize('central_longitude, kwargs, expected', [
    (0, {'dateline_direction_label': True},
     ['180°W', '120°W', '60°W', '0°', '60°E', '120°E', '180°E']),
    (180, {'zero_direction_label': True},
     ['0°E', '60°E', '120°E', '180°', '120°W', '60°W', '0°W']),
    (120, {},
     ['60°W', '0°', '60°E', '120°E', '180°', '120°W', '60°W']),
])
def test_LongitudeFormatter_central_longitude(central_longitude, kwargs,
                                              expected):
    formatter = LongitudeFormatter(**kwargs)
    p = ccrs.PlateCarree(central_longitude=central_longitude)
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-180, -120, -60, 0, 60, 120, 180]
    result = [formatter(tick) for tick in test_ticks]
    assert result == expected


def test_LongitudeFormatter_degree_symbol():
    formatter = LongitudeFormatter(degree_symbol='',
                                   dateline_direction_label=True)
    p = ccrs.PlateCarree()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-180, -120, -60, 0, 60, 120, 180]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['180W', '120W', '60W', '0', '60E', '120E', '180E']
    assert result == expected


def test_LongitudeFormatter_number_format():
    formatter = LongitudeFormatter(number_format='.2f', dms=False,
                                   dateline_direction_label=True)
    p = ccrs.PlateCarree()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-180, -120, -60, 0, 60, 120, 180]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['180.00°W', '120.00°W', '60.00°W', '0.00°',
                '60.00°E', '120.00°E', '180.00°E']
    assert result == expected


def test_LongitudeFormatter_mercator():
    formatter = LongitudeFormatter(dateline_direction_label=True)
    p = ccrs.Mercator()
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-20037508.342783064, -13358338.895188706,
                  -6679169.447594353, 0.0, 6679169.447594353,
                  13358338.895188706, 20037508.342783064]
    result = [formatter(tick) for tick in test_ticks]
    expected = ['180°W', '120°W', '60°W', '0°', '60°E', '120°E', '180°E']
    assert result == expected


@pytest.mark.parametrize('central_longitude, zero_direction_label, expected', [
    (0, False, ['17.1142343°W', '17.1142340°W', '17.1142337°W']),
    (180, True, ['162.8857657°E', '162.8857660°E', '162.8857663°E']),
])
def test_LongitudeFormatter_small_numbers(central_longitude,
                                          zero_direction_label, expected):
    formatter = LongitudeFormatter(number_format='.7f', dms=False,
                                   zero_direction_label=zero_direction_label)
    p = ccrs.PlateCarree(central_longitude=central_longitude)
    formatter.set_axis(Mock(axes=Mock(GeoAxes, projection=p)))
    test_ticks = [-17.1142343, -17.1142340, -17.1142337]
    result = [formatter(tick) for tick in test_ticks]
    assert result == expected


@pytest.mark.parametrize('direction_label', [False, True])
@pytest.mark.parametrize('test_ticks,expected', [
    pytest.param([-3.75, -3.5], ['3°45′W', '3°30′W'], id='minutes_no_hide'),
    pytest.param([-3.5, -3], ['30′', '3°W'], id='minutes_hide'),
    pytest.param([-3 - 2 * ONE_MIN - 30 * ONE_SEC], ['3°2′30″W'],
                 id='seconds'),
])
def test_LongitudeFormatter_minutes_seconds(test_ticks, direction_label,
                                            expected):
    formatter = LongitudeFormatter(dms=True, auto_hide=True,
                                   direction_label=direction_label)
    formatter.set_locs(test_ticks)
    result = [formatter(tick) for tick in test_ticks]
    prefix = '' if direction_label else '-'
    suffix = 'W' if direction_label else ''
    expected = [
        f'{prefix}{text[:-1]}{suffix}' if text[-1] == 'W' else text
        for text in expected
    ]
    assert result == expected


@pytest.mark.parametrize("test_ticks,expected", [
    pytest.param([-3.75, -3.5], ['3°45′S', '3°30′S'], id='minutes_no_hide'),
])
def test_LatitudeFormatter_minutes_seconds(test_ticks, expected):
    formatter = LatitudeFormatter(dms=True, auto_hide=True)
    formatter.set_locs(test_ticks)
    result = [formatter(tick) for tick in test_ticks]
    assert result == expected


@pytest.mark.parametrize("cls,letter",
                         [(LongitudeFormatter, 'E'), (LatitudeFormatter, 'N')])
def test_lonlatformatter_non_geoaxes(cls, letter):
    ticks = [2, 2.5]
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.plot([0, 10], [0, 1])
    ax.set_xticks(ticks)
    ax.xaxis.set_major_formatter(cls(degree_symbol='', dms=False))
    fig.canvas.draw()
    ticklabels = [t.get_text() for t in ax.get_xticklabels()]
    assert ticklabels == [f'{v:g}{letter}' for v in ticks]


@pytest.mark.parametrize("cls,vmin,vmax,expected", [
    pytest.param(LongitudeLocator, -180, 180,
                 [-180, -120, -60, 0, 60, 120, 180], id='lon_large'),
    pytest.param(LatitudeLocator, -180, 180, [-90, -60, -30, 0, 30, 60, 90],
                 id='lat_large'),
    pytest.param(LongitudeLocator, -10, 0,
                 [-10.5, -9, -7.5, -6, -4.5, -3, -1.5, 0],
                 id='lon_medium'),
    pytest.param(LongitudeLocator, -1, 0,
                 np.array([-60, -50, -40, -30, -20, -10, 0]) / 60,
                 id='lon_small'),
    pytest.param(LongitudeLocator, 0, 2 * ONE_MIN,
                 np.array([0, 18, 36, 54, 72, 90, 108, 126]) / 3600,
                 id='lon_tiny'),
])
def test_LongitudeLocator(cls, vmin, vmax, expected):
    locator = cls(dms=True)
    result = locator.tick_values(vmin, vmax)
    np.testing.assert_allclose(result, expected)


def test_lonlatformatter_decimal_point():
    xticker = LongitudeFormatter(decimal_point=',', number_format='0.2f')
    yticker = LatitudeFormatter(decimal_point=',', number_format='0.2f')
    assert xticker(-10) == "10,00°W"
    assert yticker(-10) == "10,00°S"


def test_lonlatformatter_cardinal_labels():
    xticker = LongitudeFormatter(cardinal_labels={'west': 'O'})
    yticker = LatitudeFormatter(cardinal_labels={'south': 'South'})
    assert xticker(-10) == "10°O"
    assert yticker(-10) == "10°South"
