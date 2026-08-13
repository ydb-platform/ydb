LIBRARY()

SRCS(
    activeactors.cpp
    activeactors.h
    future_callback.h
    mon_histogram_helper.h
    selfping_actor.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/util
    library/cpp/monlib/dynamic_counters
)

END()

RECURSE_FOR_TESTS(
    ut
)
