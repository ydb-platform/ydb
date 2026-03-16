import logging
import sys

import pytest

from shapely.geometry import LineString
from shapely.errors import ReadingError
from shapely.wkt import loads


def test_error_handler_exception(tmpdir):
    """Error logged in addition to exception"""
    logger = logging.getLogger('shapely.geos')
    logfile = str(tmpdir.join('test_error.log'))
    fh = logging.FileHandler(logfile)
    logger.addHandler(fh)

    # This calls error_handler with a format string of "%s" and one
    # value.
    with pytest.raises(ReadingError):
        loads('POINT (LOLWUT)')

    log = open(logfile).read()
    assert "Expected number but encountered word: 'LOLWUT'" in log


def test_error_handler(tmpdir):
    logger = logging.getLogger('shapely.geos')
    logfile = str(tmpdir.join('test_error.log'))
    fh = logging.FileHandler(logfile)
    logger.addHandler(fh)

    # This operation calls error_handler with a format string that
    # has *no* conversion specifiers.
    LineString([(0, 0), (2, 2)]).project(LineString([(1, 1), (1.5, 1.5)]))

    log = open(logfile).read()
    assert "third argument of GEOSProject_r must be Point" in log


def test_error_handler_wrong_type():
    with pytest.raises(TypeError):
        loads(1)


@pytest.mark.skipif(sys.version_info[0] < 3, reason='accept str and unicode for python2 and str for python3')
def test_error_handler_for_bytes():
    with pytest.raises(TypeError):
        loads(b'POINT (10 10)')


def test_info_handler(tmpdir):
    logger = logging.getLogger('shapely.geos')
    logfile = str(tmpdir.join('test_error.log'))
    fh = logging.FileHandler(logfile)
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    g = loads('MULTIPOLYGON (((10 20, 10 120, 60 70, 30 70, 30 40, 60 40, 60 70, 90 20, 10 20)))')
    assert not g.is_valid

    log = open(logfile).read()
    assert "Ring Self-intersection at or near point 60 70" in log


def test_info_handler_quiet(tmpdir):
    logger = logging.getLogger('shapely.geos')
    logfile = str(tmpdir.join('test_error.log'))
    fh = logging.FileHandler(logfile)
    logger.addHandler(fh)
    logger.setLevel(logging.WARNING)

    g = loads('MULTIPOLYGON (((10 20, 10 120, 60 70, 30 70, 30 40, 60 40, 60 70, 90 20, 10 20)))')
    assert not g.is_valid

    log = open(logfile).read()
    assert "Ring Self-intersection at or near point 60 70" not in log
