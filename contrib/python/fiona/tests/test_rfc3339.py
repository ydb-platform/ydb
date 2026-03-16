"""Tests for Fiona's RFC 3339 support."""


import re

import pytest

from fiona.rfc3339 import parse_date, parse_datetime, parse_time
from fiona.rfc3339 import group_accessor, pattern_date


class TestDateParse:

    def test_yyyymmdd(self):
        assert parse_date("2012-01-29") == (2012, 1, 29, 0, 0, 0, 0.0, None)

    def test_error(self):
        with pytest.raises(ValueError):
            parse_date("xxx")


class TestTimeParse:

    def test_hhmmss(self):
        assert parse_time("10:11:12") == (0, 0, 0, 10, 11, 12, 0.0, None)

    def test_hhmm(self):
        assert parse_time("10:11") == (0, 0, 0, 10, 11, 0, 0.0, None)

    def test_hhmmssff(self):
        assert parse_time("10:11:12.42") == (0, 0, 0, 10, 11, 12, 0.42*1000000, None)

    def test_hhmmssz(self):
        assert parse_time("10:11:12Z") == (0, 0, 0, 10, 11, 12, 0.0, None)

    def test_hhmmssoff(self):
        assert parse_time("10:11:12-01:30") == (0, 0, 0, 10, 11, 12, 0.0, -90)

    def test_hhmmssoff2(self):
        assert parse_time("10:11:12+01:30") == (0, 0, 0, 10, 11, 12, 0.0, 90)

    def test_error(self):
        with pytest.raises(ValueError):
            parse_time("xxx")


class TestDatetimeParse:

    def test_yyyymmdd(self):
        assert (
            parse_datetime("2012-01-29T10:11:12") ==
            (2012, 1, 29, 10, 11, 12, 0.0, None))

    def test_yyyymmddTZ(self):
        assert (
            parse_datetime("2012-01-29T10:11:12+01:30") ==
            (2012, 1, 29, 10, 11, 12, 0.0, 90))

    def test_yyyymmddTZ2(self):
        assert (
            parse_datetime("2012-01-29T10:11:12-01:30") ==
            (2012, 1, 29, 10, 11, 12, 0.0, -90))

    def test_error(self):
        with pytest.raises(ValueError):
            parse_datetime("xxx")


def test_group_accessor_indexerror():
    match = re.search(pattern_date, '2012-01-29')
    g = group_accessor(match)
    assert g.group(-1) == 0
    assert g.group(6) == 0
