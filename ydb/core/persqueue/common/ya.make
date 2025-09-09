LIBRARY()

SRCS(
    common_app.cpp
    heartbeat.cpp
    key.cpp
    microseconds_sliding_window.cpp
)

GENERATE_ENUM_SERIALIZATION(sourceid_info.h)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/persqueue/public
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
