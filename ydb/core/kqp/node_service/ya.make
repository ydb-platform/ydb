LIBRARY()

SRCS(
    kqp_node_service.cpp
    kqp_node_state.cpp
    kqp_query_control_plane.cpp
)

PEERDIR(
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/cms/console/events
    ydb/core/kqp/common
    ydb/core/kqp/compute_actor
    ydb/core/kqp/counters
    ydb/core/kqp/runtime/scheduler
    ydb/core/protos
    ydb/core/tablet
    ydb/library/actors/async
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
)

YQL_LAST_ABI_VERSION()

END()
