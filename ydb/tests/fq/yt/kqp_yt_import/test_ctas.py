from ydb.tests.fq.tools.kqprun import KqpRun
from ydb.tests.fq.yt.kqp_yt_import.helpers import add_sample_table, validate_sample_result


class TestYtCtas:
    def test_simple_ctast(self, kqp_run: KqpRun):
        add_sample_table(kqp_run)

        kqp_run.add_query("""
            CREATE TABLE from_yt (
                PRIMARY KEY (key)
            ) WITH (
                STORE = COLUMN
            )
            AS SELECT UNWRAP(key) AS key, subkey, value FROM plato.input
        """)

        kqp_run.add_query("""
            SELECT
                UNWRAP(key) AS key, UNWRAP(subkey) AS subkey, UNWRAP(value) AS value
            FROM from_yt
            ORDER BY subkey
        """)

        result = kqp_run.yql_exec(verbose=True)
        validate_sample_result(result.results)
