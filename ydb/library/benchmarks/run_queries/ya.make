PY3_PROGRAM(run_queries)

PY_SRCS(
    run_queries.py
)

PEERDIR(
    runner
)

DEPENDS(
    ydb/library/yql/tools/dqrun
    ydb/library/yql/udfs/common/set
    ydb/library/yql/udfs/common/url_base
    ydb/library/yql/udfs/common/datetime2
    ydb/library/yql/udfs/common/re2
)

END()