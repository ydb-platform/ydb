LIBRARY()

SRCS(
    common_app.cpp
    key.cpp
    microseconds_sliding_window.cpp
)

GENERATE_ENUM_SERIALIZATION(sourceid_info.h)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/persqueue/public
)

END()

RECURSE(
    proxy
)

RECURSE_FOR_TESTS(
    ut
)
