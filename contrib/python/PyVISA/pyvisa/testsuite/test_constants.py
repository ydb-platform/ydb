# -*- coding: utf-8 -*-
"""Test objects from constants.

This file is part of PyVISA.

:copyright: 2019-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import pytest

from pyvisa.constants import DataWidth

from . import BaseTestCase


class TestDataWidth(BaseTestCase):
    def test_conversion_from_literal(self):
        for v, e in zip(
            (8, 16, 32, 64),
            (DataWidth.bit_8, DataWidth.bit_16, DataWidth.bit_32, DataWidth.bit_64),
        ):
            assert DataWidth.from_literal(v) == e

        with pytest.raises(ValueError):
            DataWidth.from_literal(0)
