# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbVectorWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        feature_flags_str = yatest.common.get_param('feature_flags', default='')
        extra_flags = [f for f in feature_flags_str.split(',') if f]
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
        mode = yatest.common.get_param('vector_mode', default='standalone')
        data_dir = yatest.common.get_param('vector_data_dir', default=None)
        targets = yatest.common.get_param('vector_targets', default='10000')
        warmup = yatest.common.get_param('vector_warmup', default='0')
        rows = yatest.common.get_param('vector_rows', default='100000')
        threads = yatest.common.get_param('vector_threads', default='10')

        cmd = [
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
            "--mode", mode,
            "--targets", targets,
            "--warmup", warmup,
            "--rows", rows,
            "--threads", threads,
        ]
        if data_dir:
            cmd.extend(["--data-dir", data_dir])

        yatest.common.execute(cmd)
