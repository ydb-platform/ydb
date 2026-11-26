YQL_LIBRARY()

SRCS(
    channel_storage_actor.cpp
    channel_storage.cpp
    compute_storage_actor.cpp
    compute_storage.cpp
    spilling_file.cpp
    spilling.cpp
)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/library/actors/core
    ydb/library/actors/util
    ydb/library/services
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/actors/spilling/counters
    ydb/library/yql/dq/common
    ydb/library/yql/dq/runtime
    yql/essentials/utils
)

END()

RECURSE(
    counters
)

RECURSE_FOR_TESTS(
    ut
)
