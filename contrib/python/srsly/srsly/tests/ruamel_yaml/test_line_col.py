# coding: utf-8

import pytest  # NOQA

from .roundtrip import round_trip, dedent, round_trip_load, round_trip_dump  # NOQA


def load(s):
    return round_trip_load(dedent(s))


class TestLineCol:
    def test_item_00(self):
        data = load(
            """
            - a
            - e
            - [b, d]
            - c
            """
        )
        assert data[2].lc.line == 2
        assert data[2].lc.col == 2

    def test_item_01(self):
        data = load(
            """
            - a
            - e
            - {x: 3}
            - c
            """
        )
        assert data[2].lc.line == 2
        assert data[2].lc.col == 2

    def test_item_02(self):
        data = load(
            """
            - a
            - e
            - !!set {x, y}
            - c
            """
        )
        assert data[2].lc.line == 2
        assert data[2].lc.col == 2

    def test_item_03(self):
        data = load(
            """
            - a
            - e
            - !!omap
              - x: 1
              - y: 3
            - c
            """
        )
        assert data[2].lc.line == 2
        assert data[2].lc.col == 2

    def test_item_04(self):
        data = load(
            """
         # testing line and column based on SO
         # http://stackoverflow.com/questions/13319067/
         - key1: item 1
           key2: item 2
         - key3: another item 1
           key4: another item 2
            """
        )
        assert data[0].lc.line == 2
        assert data[0].lc.col == 2
        assert data[1].lc.line == 4
        assert data[1].lc.col == 2

    def test_pos_mapping(self):
        data = load(
            """
        a: 1
        b: 2
        c: 3
        # comment
        klm: 42
        d: 4
        """
        )
        assert data.lc.key("klm") == (4, 0)
        assert data.lc.value("klm") == (4, 5)

    def test_pos_sequence(self):
        data = load(
            """
        - a
        - b
        - c
        # next one!
        - klm
        - d
        """
        )
        assert data.lc.item(3) == (4, 2)
