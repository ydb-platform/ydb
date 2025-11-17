# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbKvWorkload:
    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test(self, store_type):
        oom = []
        while True:
            oom.append("x" * 1024 * 1024 * 100)
