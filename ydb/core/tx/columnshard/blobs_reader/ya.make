LIBRARY()

SRCS(
    actor.cpp
    task.cpp
)

PEERDIR(
    ydb/core/formats/arrow/reader/stats
    library/cpp/retry
    ydb/core/protos
    ydb/library/actors/core
    ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
