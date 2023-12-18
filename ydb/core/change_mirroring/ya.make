LIBRARY()

PEERDIR(
    ydb/library/actors/core
)

SRCS(
    ydb/core/change_mirroring/reader.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
