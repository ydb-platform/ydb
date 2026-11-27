YQL_LIBRARY()

SRCS(
    kqp_compute_context.cpp
    kqp_program_builder.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/simple
    ydb/core/scheme
    ydb/library/aclib
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/runtime
    yql/essentials/minikql
)

END()
