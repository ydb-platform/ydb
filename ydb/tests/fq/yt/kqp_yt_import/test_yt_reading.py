import json

from ydb.tests.fq.tools.kqprun import KqpRun
from ydb.tests.fq.yt.kqp_yt_import.helpers import add_sample_table, validate_sample_result


class TestYtReading:
    def test_partitioned_reading(self, kqp_run: KqpRun):
        add_sample_table(kqp_run)

        kqp_run.add_query("""
            PRAGMA ydb.DataSizePerPartition = "1";

            SELECT * FROM plato.input
        """)

        result = kqp_run.yql_exec(verbose=True)

        plan = json.loads(result.plan)
        assert plan['Plan']['Plans'][0]['Plans'][0]['Plans'][0]['Plans'][0]['Stats']['Tasks'] == 2

    def test_block_reading(self, kqp_run: KqpRun):
        add_sample_table(kqp_run, infer_schema=False)

        kqp_run.add_query("""
            PRAGMA UseBlocks;
            PRAGMA BlockEngine = "force";
            PRAGMA ydb.UseBlockReader = "true";

            SELECT
                key, subkey, value
            FROM plato.input
            ORDER BY subkey
        """)

        result = kqp_run.yql_exec(verbose=True)
        validate_sample_result(result.results)

        assert "DqReadBlockWideWrap" in result.opt, result.opt
