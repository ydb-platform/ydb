LIBRARY()

SRCS(
    cpu_quota_manager.cpp
    events.cpp
    helpers.cpp
)

PEERDIR(
    ydb/core/kqp/common/events

    ydb/core/scheme

    ydb/core/tx/scheme_cache

    ydb/library/actors/core

    ydb/public/sdk/cpp/client/ydb_types

    library/cpp/retry
)

YQL_LAST_ABI_VERSION()

END()
