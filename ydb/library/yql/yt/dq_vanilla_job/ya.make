PROGRAM()

PEERDIR(
    library/cpp/svnversion
    library/cpp/yt/mlock
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/integration/transform
    ydb/library/yql/dq/transform
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/providers/yt/comp_nodes/llvm14
    ydb/library/yql/utils/backtrace
    ydb/library/yql/providers/yt/comp_nodes/dq
    ydb/library/yql/providers/yt/mkql_dq
    ydb/library/yql/tools/dq/worker_job
    ydb/library/yql/sql/pg
    ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
