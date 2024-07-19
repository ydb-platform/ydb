LIBRARY()

SRCS(
    kqp_compute_actor_factory.cpp
    kqp_compute_actor.cpp
    kqp_compute_actor_helpers.cpp
    kqp_compute_events.cpp
    kqp_compute_state.cpp
    kqp_pure_compute_actor.cpp
    kqp_scan_compute_stat.cpp
    kqp_scan_compute_manager.cpp
    kqp_scan_compute_actor.cpp
    kqp_scan_fetcher_actor.cpp
    kqp_scan_common.cpp
    kqp_scan_events.cpp
)

PEERDIR(
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/kqp/federated_query
    ydb/core/kqp/runtime
    ydb/core/tx/datashard
    ydb/core/tx/scheme_cache
    ydb/core/formats/arrow/protos
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/generic/actors
    ydb/library/yql/providers/s3/actors_factory
    ydb/library/yql/public/issue
)

GENERATE_ENUM_SERIALIZATION(kqp_compute_state.h)
YQL_LAST_ABI_VERSION()

END()
