# coding: utf-8

from __future__ import print_function, absolute_import, division, unicode_literals

import pytest  # NOQA

from .roundtrip import dedent, round_trip_load, round_trip_dump

# http://yaml.org/type/int.html is where underscores in integers are defined


class TestBinHexOct:
    def test_calculate(self):
        # make sure type, leading zero(s) and underscore are preserved
        s = dedent(
            """\
        - 42
        - 0b101010
        - 0x_2a
        - 0x2A
        - 0o00_52
        """
        )
        d = round_trip_load(s)
        for idx, elem in enumerate(d):
            elem -= 21
            d[idx] = elem
        for idx, elem in enumerate(d):
            elem *= 2
            d[idx] = elem
        for idx, elem in enumerate(d):
            t = elem
            elem **= 2
            elem //= t
            d[idx] = elem
        assert round_trip_dump(d) == s
