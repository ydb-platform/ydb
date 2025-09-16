# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbLogWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            column_shard_config={
                'disabled_on_scheme_shard': False,
            })

    @pytest.mark.parametrize('store_type', ['row', 'column'])
    def test(self, store_type):
        a = 42
        b = 43
        assert a > b
