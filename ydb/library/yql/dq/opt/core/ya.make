LIBRARY()

SRCDIR(ydb/library/yql/dq/opt)

SRCS(
    dq_opt.cpp
    dq_opt_build.cpp
    dq_opt_hopping.cpp
    dq_opt_log.cpp
    dq_opt_peephole.cpp
    dq_opt_phy.cpp
    dq_opt_phy_finalizing.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/dq_integration
    yql/essentials/parser/pg_wrapper/interface
    ydb/library/yql/dq/common
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/type_ann
    ydb/library/yql/providers/dq/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
