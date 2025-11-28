LIBRARY()

SRCS(
    actor.cpp
    common_app.cpp
    heartbeat.cpp
    key.cpp
    microseconds_sliding_window.cpp
)

GENERATE_ENUM_SERIALIZATION(sourceid_info.h)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/persqueue/public
    ydb/core/persqueue/public/partition_key_range
    ydb/library/actors/core
    ydb/library/logger
)

END()

RECURSE(
    proxy
)

RECURSE_FOR_TESTS(
    ut
)
