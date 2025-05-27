PY3TEST()

SIZE(LARGE)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

TEST_SRCS(
    tpc_tests.py
)

DEPENDS(
    ydb/library/benchmarks/runner/run_tests
    ydb/library/yql/tools/dqrun
    ydb/library/benchmarks/gen_queries
    ydb/library/benchmarks/runner/result_compare
    ydb/library/benchmarks/runner/runner

    yql/essentials/udfs/common/set
    yql/essentials/udfs/common/url_base
    yql/essentials/udfs/common/datetime2
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/math
    yql/essentials/udfs/common/unicode_base
)

DATA_FILES(
    ydb/library/yql/tools/dqrun/examples/fs.conf
    ydb/library/benchmarks/runner/runner/test-gateways.conf
    contrib/tools/flame-graph

    ydb/library/benchmarks/runner/download_lib.sh
    ydb/library/benchmarks/runner/download_tables.sh
    ydb/library/benchmarks/runner/download_tpcds_tables.sh
    ydb/library/benchmarks/runner/download_files_ds_1.sh
    ydb/library/benchmarks/runner/download_files_ds_10.sh
    ydb/library/benchmarks/runner/download_files_ds_100.sh
    ydb/library/benchmarks/runner/download_files_h_1.sh
    ydb/library/benchmarks/runner/download_files_h_10.sh
    ydb/library/benchmarks/runner/download_files_h_100.sh

    ydb/library/benchmarks/runner/upload_results.py
)

END()

RECURSE(
    run_tests
    runner
    result_convert
    result_compare
)
