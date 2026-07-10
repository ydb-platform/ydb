# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbFulltextWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        feature_flags_str = yatest.common.get_param('feature_flags', default='')
        extra_flags = [f for f in feature_flags_str.split(',') if f]
        extra_flags.append("enable_fulltext_index")
        tsc_str = yatest.common.get_param('table_service_config', default='')
        tsc = {}
        for item in tsc_str.split(','):
            if '=' in item:
                k, v = item.split('=', 1)
                if v.lower() == 'true':
                    tsc[k] = True
                elif v.lower() == 'false':
                    tsc[k] = False
                else:
                    tsc[k] = v
        yield from self.setup_cluster(extra_feature_flags=extra_flags, table_service_config=tsc or None)

    def test(self):
        rows = yatest.common.get_param('fulltext_rows', default='10000')
        targets = yatest.common.get_param('fulltext_targets', default='1000')
        threads = yatest.common.get_param('fulltext_threads', default='10')

        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
            "--rows", rows,
            "--targets", targets,
            "--threads", threads,
        ])
