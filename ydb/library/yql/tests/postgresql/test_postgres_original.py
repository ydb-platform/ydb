import yatest.common
from common import find_sql_tests, run_sql_test

DATA_PATH = yatest.common.test_source_path('original/cases')
PGRUN = yatest.common.binary_path('ydb/library/yql/tools/pgrun/pgrun')
UDFS = [
    yatest.common.binary_path('ydb/library/yql/udfs/common/set/libset_udf.so'),
    yatest.common.binary_path('ydb/library/yql/udfs/common/re2/libre2_udf.so'),
]


def pytest_generate_tests(metafunc):
    ids, tests = zip(*find_sql_tests(DATA_PATH))
    metafunc.parametrize(['sql', 'out'], tests, ids=ids)


def test(sql, out, tmp_path):
    run_sql_test(sql, out, tmp_path, PGRUN, UDFS)
