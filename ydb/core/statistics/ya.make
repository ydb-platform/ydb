LIBRARY()

SRCS(
    events.h
    stat_service.h
    stat_service.cpp
)

PEERDIR(
    util
    library/cpp/actors/core
    ydb/core/protos
    ydb/core/scheme
)

END()

RECURSE_FOR_TESTS(
    ut
)
