LIBRARY()

SRCS(
    events.cpp
    helpers.cpp
)

PEERDIR(
    ydb/core/kqp/common/events

    ydb/core/scheme

    ydb/core/tx/scheme_cache

    ydb/library/actors/core

    library/cpp/retry
)

YQL_LAST_ABI_VERSION()

END()
