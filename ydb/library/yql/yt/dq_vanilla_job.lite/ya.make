PROGRAM()

PEERDIR(
    library/cpp/yt/mlock
    yt/cpp/mapreduce/client
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/utils/backtrace
    ydb/library/yql/dq/comp_nodes
    yql/essentials/core/dq_integration/transform
    ydb/library/yql/dq/transform
    ydb/library/yql/dq/runtime
    yql/essentials/providers/common/comp_nodes
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/runtime
    yt/yql/providers/yt/comp_nodes/dq
    yt/yql/providers/yt/mkql_dq
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm14
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
