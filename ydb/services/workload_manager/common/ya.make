LIBRARY()

SRCS(
    cpu_quota_manager.cpp
    events.cpp
    helpers.cpp
)

PEERDIR(

    ydb/core/base
    ydb/core/scheme


    ydb/library/actors/core

    ydb/public/sdk/cpp/src/client/types

    library/cpp/retry
)

YQL_LAST_ABI_VERSION()

END()
