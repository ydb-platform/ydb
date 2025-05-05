import os
import pytest

from ydb.tests.fq.tools.kqprun import KqpRun


@pytest.fixture
def kqp_run(request) -> KqpRun:
    return KqpRun(
        config_file=os.path.join('ydb/tests/fq/yt/kqp_yt_import', 'kqprun_import_config.conf'),
        scheme_file=os.path.join('ydb/tests/fq/yt/cfg', 'kqprun_scheme.sql'),
        path_prefix=f"{request.function.__name__}_"
    )
