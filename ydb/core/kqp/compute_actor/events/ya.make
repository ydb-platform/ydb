LIBRARY()

SRCS(
    kqp_compute_events.cpp
    kqp_compute_events.h
    kqp_compute_events_stats.cpp
    kqp_compute_events_stats.h
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/formats
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/library/actors/core
    ydb/library/formats/arrow
)

YQL_LAST_ABI_VERSION()

END()
