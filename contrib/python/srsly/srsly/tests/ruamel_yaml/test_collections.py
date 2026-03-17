# coding: utf-8

"""
collections.OrderedDict is a new class not supported by PyYAML (issue 83 by Frazer McLean)

This is now so integrated in Python that it can be mapped to !!omap

"""

import pytest  # NOQA


from .roundtrip import round_trip, dedent, round_trip_load, round_trip_dump  # NOQA


class TestOrderedDict:
    def test_ordereddict(self):
        from collections import OrderedDict
        import srsly.ruamel_yaml  # NOQA

        assert srsly.ruamel_yaml.dump(OrderedDict()) == "!!omap []\n"
