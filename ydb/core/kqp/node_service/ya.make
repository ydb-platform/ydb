LIBRARY()

SRCS(
    kqp_node_service.cpp
    kqp_node_state.cpp  # gn: state
    kqp_query_control_plane.cpp
)
# gn: state headers kqp_node_state.h

PEERDIR(
    ydb/library/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
    contrib/libs/tcmalloc/malloc_extension
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/compute_actor
    ydb/core/kqp/counters
    ydb/core/mind
    ydb/core/protos
    ydb/core/tablet
    ydb/library/yql/dq/actors/compute
    ydb/library/actors/async
)

YQL_LAST_ABI_VERSION()

END()
