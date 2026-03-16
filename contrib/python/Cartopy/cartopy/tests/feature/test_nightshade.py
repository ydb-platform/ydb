# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from datetime import datetime

import pytest

from cartopy.feature.nightshade import Nightshade, _julian_day, _solar_position


def test_julian_day():
    # Using Vallado 2007 "Fundamentals of Astrodynamics and Applications"
    # Example 3.4
    dt = datetime(1996, 10, 26, 14, 20)
    jd = _julian_day(dt)
    assert pytest.approx(jd) == 2450383.09722222


# Using timeanddate website. Not very many digits of accuracy,
# but it should do OK for simple testing
# 1) Early Sept 2018
#    https://www.timeanddate.com/worldclock/sunearth.html
#    ?month=9&day=29&year=2018&hour=00&min=00&sec=0&n=&ntxt=&earth=0
# 2) Later Sept 2018
#    https://www.timeanddate.com/worldclock/sunearth.html
#    ?month=9&day=29&year=2018&hour=14&min=00&sec=0&n=&ntxt=&earth=0
# 3) Different year/month (Feb 1992)
#    https://www.timeanddate.com/worldclock/sunearth.html
#    ?month=2&day=14&year=1992&hour=00&min=0&sec=0&n=&ntxt=&earth=0
# 4) Test in the future near summer solstice (June 2030)
#    https://www.timeanddate.com/worldclock/sunearth.html
#    ?month=6&day=21&year=2030&hour=0&min=0&sec=0&n=&ntxt=&earth=0

@pytest.mark.parametrize('dt, true_lat, true_lon', [
    (datetime(2018, 9, 29, 0, 0), -(2 + 18 / 60), (177 + 37 / 60)),
    (datetime(2018, 9, 29, 14, 0), -(2 + 32 / 60), -(32 + 25 / 60)),
    (datetime(1992, 2, 14, 0, 0), -(13 + 20 / 60), -(176 + 26 / 60)),
    (datetime(2030, 6, 21, 0, 0), (23 + 26 / 60), -(179 + 34 / 60))
])
def test_solar_position(dt, true_lat, true_lon):
    lon, lat = _solar_position(dt)
    assert pytest.approx(true_lat, 0.1) == lat
    assert pytest.approx(true_lon, 0.1) == lon


def test_nightshade_floating_point():
    # Smoke test for clipping nightshade floating point values
    date = datetime(1999, 12, 31, 12)

    # This can cause an error with floating point precision if it is
    # set to exactly -6 and arccos input is not clipped to [-1, 1]
    Nightshade(date, refraction=-6.0, color='none')
