PY3TEST()

SIZE(MEDIUM)

TEST_SRCS(
    tpc_tests.py
)

DEPENDS(
    ydb/library/benchmarks/runner/run_tests
    ydb/library/yql/tools/dqrun
    ydb/library/benchmarks/gen_queries
    ydb/library/benchmarks/runner/result_compare
    ydb/library/benchmarks/runner/runner

    ydb/library/yql/udfs/common/set
    ydb/library/yql/udfs/common/url_base
    ydb/library/yql/udfs/common/datetime2
    ydb/library/yql/udfs/common/re2
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
)

END()

RECURSE(
    run_tests
    runner
    result_convert
    result_compare
)
