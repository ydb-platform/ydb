# coding: utf-8

from __future__ import print_function, absolute_import, division, unicode_literals

import pytest  # NOQA

from .roundtrip import round_trip, dedent, round_trip_load, round_trip_dump  # NOQA

# http://yaml.org/type/int.html is where underscores in integers are defined


class TestFloat:
    def test_round_trip_non_exp(self):
        data = round_trip(
            """\
        - 1.0
        - 1.00
        - 23.100
        - -1.0
        - -1.00
        - -23.100
        - 42.
        - -42.
        - +42.
        - .5
        - +.5
        - -.5
        """
        )
        print(data)
        assert 0.999 < data[0] < 1.001
        assert 0.999 < data[1] < 1.001
        assert 23.099 < data[2] < 23.101
        assert 0.999 < -data[3] < 1.001
        assert 0.999 < -data[4] < 1.001
        assert 23.099 < -data[5] < 23.101
        assert 41.999 < data[6] < 42.001
        assert 41.999 < -data[7] < 42.001
        assert 41.999 < data[8] < 42.001
        assert 0.49 < data[9] < 0.51
        assert 0.49 < data[10] < 0.51
        assert -0.51 < data[11] < -0.49

    def test_round_trip_zeros_0(self):
        data = round_trip(
            """\
        - 0.
        - +0.
        - -0.
        - 0.0
        - +0.0
        - -0.0
        - 0.00
        - +0.00
        - -0.00
        """
        )
        print(data)
        for d in data:
            assert -0.00001 < d < 0.00001

    def Xtest_round_trip_non_exp_trailing_dot(self):
        data = round_trip(
            """\
        """
        )
        print(data)

    def test_yaml_1_1_no_dot(self):
        from srsly.ruamel_yaml.error import MantissaNoDotYAML1_1Warning

        with pytest.warns(MantissaNoDotYAML1_1Warning):
            round_trip_load(
                """\
            %YAML 1.1
            ---
            - 1e6
            """
            )


class TestCalculations(object):
    def test_mul_00(self):
        # issue 149 reported by jan.brezina@tul.cz
        d = round_trip_load(
            """\
        - 0.1
        """
        )
        d[0] *= -1
        x = round_trip_dump(d)
        assert x == "- -0.1\n"
