PY3_PROGRAM()

PY_SRCS(
    MAIN runner.py
)

PEERDIR(
    ydb/library/yql/tools/dqrun
    ydb/library/benchmarks/gen_queries

    ydb/library/yql/udfs/common/set
    ydb/library/yql/udfs/common/url_base
    ydb/library/yql/udfs/common/datetime2
    ydb/library/yql/udfs/common/re2
)

END()
