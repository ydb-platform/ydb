LIBRARY()

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/core
    ydb/library/yql/core/type_ann
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/common/provider
)

SRCS(
    dq_type_ann.cpp
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(dq_type_ann.h)

END()
