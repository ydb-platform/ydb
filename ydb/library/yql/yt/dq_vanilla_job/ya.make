PROGRAM()

PEERDIR(
    library/cpp/svnversion
    library/cpp/yt/mlock
    ydb/library/yql/dq/comp_nodes
    yql/essentials/core/dq_integration/transform
    ydb/library/yql/dq/transform
    yql/essentials/providers/common/comp_nodes
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/providers/yt/comp_nodes/llvm14
    yql/essentials/utils/backtrace
    ydb/library/yql/providers/yt/comp_nodes/dq
    ydb/library/yql/providers/yt/mkql_dq
    ydb/library/yql/tools/dq/worker_job
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
