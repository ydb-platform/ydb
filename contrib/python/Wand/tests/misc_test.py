import datetime
import numbers
import re

from pytest import mark, raises

from wand.version import (MAGICK_RELEASE_DATE, MAGICK_RELEASE_DATE_STRING,
                          MAGICK_VERSION, MAGICK_VERSION_INFO,
                          MAGICK_VERSION_NUMBER, QUANTUM_DEPTH,
                          configure_options, fonts, formats)


def test_version():
    """Test version strings."""
    match = re.match(r'^ImageMagick\s+\d+\.\d+\.\d+(?:-\d+)?', MAGICK_VERSION)
    assert match
    assert isinstance(MAGICK_VERSION_INFO, tuple)
    assert (len(MAGICK_VERSION_INFO) ==
            match.group(0).count('.') + match.group(0).count('-') + 1)
    assert all(isinstance(v, int) for v in MAGICK_VERSION_INFO)
    assert isinstance(MAGICK_VERSION_NUMBER, numbers.Integral)
    assert isinstance(MAGICK_RELEASE_DATE, datetime.date)
    date_strings = (MAGICK_RELEASE_DATE.strftime('%Y-%m-%d'),
                    MAGICK_RELEASE_DATE.strftime('%Y%m%d'))
    assert MAGICK_RELEASE_DATE_STRING in date_strings


def test_quantum_depth():
    """QUANTUM_DEPTH must be one of 8, 16, 32, or 64."""
    assert QUANTUM_DEPTH in (8, 16, 32, 64)


@mark.xfail(reason='No configure options')
def test_configure_options():
    assert 'RELEASE_DATE' in configure_options('RELEASE_DATE')
    with raises(TypeError):
        configure_options(0xDEADBEEF)


def test_fonts():
    font_list = fonts()
    if not font_list:
        mark.skip('Fonts not configured on system')
    else:
        first_font = font_list[0]
        first_font_part = first_font[1:-1]
        assert first_font in fonts('*{0}*'.format(first_font_part))
    with raises(TypeError):
        fonts(0xDEADBEEF)


def test_formats():
    xc = 'XC'
    assert formats(xc) == [xc]
    with raises(TypeError):
        formats(0xDEADBEEF)
