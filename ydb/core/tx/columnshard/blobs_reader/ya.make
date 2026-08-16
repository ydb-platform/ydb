LIBRARY()

SRCS(
    actor.cpp
    task.cpp
)

PEERDIR(
    library/cpp/retry
    ydb/core/protos
    ydb/library/actors/core
    ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
