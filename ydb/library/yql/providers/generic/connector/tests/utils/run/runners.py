from typing import Final

import yatest.common as yat

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings

from ydb.library.yql.providers.generic.connector.tests.utils.run.parent import Runner
from ydb.library.yql.providers.generic.connector.tests.utils.run.dqrun import DqRunner
from ydb.library.yql.providers.generic.connector.tests.utils.run.kqprun import KqpRunner

# used in every test.py
runner_types: Final = ("dqrun", "kqprun")


# used in every test.py
def configure_runner(runner_type: str, settings: Settings) -> Runner:
    match runner_type:
        case "dqrun":
            return DqRunner(
                dqrun_path=yat.build_path("ydb/library/yql/tools/dqrun/dqrun"),
                settings=settings,
                udf_dir=yat.build_path("ydb/library/yql/udfs/common/json2"),
            )
        case "kqprun":
            return KqpRunner(
                kqprun_path=yat.build_path("ydb/tests/tools/kqprun/kqprun"),
                settings=settings,
                udf_dir=yat.build_path("ydb/library/yql/udfs/common/json2"),
            )
        case _:
            raise ValueError(runner_type)
