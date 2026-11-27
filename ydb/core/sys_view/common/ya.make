LIBRARY()

SRCS(
    common.h
    events.h
    keys.h
    path.h
    query_metrics_limits.h
    registry.cpp
    resolver.cpp
    utils.h
)

PEERDIR(
    library/cpp/deprecated/atomic
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/library/actors/core
)

END()
