LIBRARY()

SRCS(
    common.h
    events.h
    keys.h
    path.h
    query_metrics_limits.h
    scan_actor_base_impl.h
    registry.cpp
    resolver.cpp
    utils.h
    processor_scan.h
)

PEERDIR(
    library/cpp/deprecated/atomic
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime/common
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/library/actors/core
)

END()
