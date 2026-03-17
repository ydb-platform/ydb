import pytest

from babel.units import format_unit


# New units in CLDR 46
@pytest.mark.parametrize(('unit', 'count', 'expected'), [
    ('speed-light-speed', 1, '1 světlo'),
    ('speed-light-speed', 2, '2 světla'),
    ('speed-light-speed', 5, '5 světel'),
    ('concentr-portion-per-1e9', 1, '1 částice na miliardu'),
    ('concentr-portion-per-1e9', 2, '2 částice na miliardu'),
    ('concentr-portion-per-1e9', 5, '5 částic na miliardu'),
    ('duration-night', 1, '1 noc'),
    ('duration-night', 2, '2 noci'),
    ('duration-night', 5, '5 nocí'),
])
def test_new_cldr46_units(unit, count, expected):
    assert format_unit(count, unit, locale='cs_CZ') == expected
