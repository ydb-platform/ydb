PROGRAM()

PEERDIR(
    library/cpp/svnversion
    library/cpp/yt/mlock
    ydb/library/yql/dq/comp_nodes
    yql/essentials/core/dq_integration/transform
    ydb/library/yql/dq/transform
    yql/essentials/providers/common/comp_nodes
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm14
    yql/essentials/utils/backtrace
    yt/yql/providers/yt/comp_nodes/dq
    yt/yql/providers/yt/mkql_dq
    ydb/library/yql/tools/dq/worker_job
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
