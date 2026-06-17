PROGRAM(cbo_latency_dataset)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/kqp
    ydb/core/kqp/opt/cbo/bench
    ydb/core/kqp/opt/cbo/solver
    ydb/core/kqp/ut/common
    ydb/public/lib/ydb_cli/common
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/digest
)

YQL_LAST_ABI_VERSION()

END()
