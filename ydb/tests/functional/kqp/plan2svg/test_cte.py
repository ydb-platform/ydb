import yatest.common


class TestCte:
    def test_toplevel(self):
        yatest.common.execute(
            [
                yatest.common.binary_path("ydb/tests/tools/kqprun/kqprun"),
                "-c",
                yatest.common.source_path("ydb/tests/tools/kqprun/configuration/app_config.conf"),
                "--sql",
                "SELECT 1; SELECT 1;",
                "--script-timeline-file",
                "timeline.svg",
            ]
        )
