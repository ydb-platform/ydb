LIBRARY()

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/type_ann
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/proto
    yql/essentials/providers/common/provider
)

SRCS(
    dq_type_ann.cpp
)

CHECK_DEPENDENT_DIRS(DENY PEERDIRS
    ydb/core/kqp/opt/cbo
    ydb/core/kqp/opt/cbo/solver
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(dq_type_ann.h)

END()
