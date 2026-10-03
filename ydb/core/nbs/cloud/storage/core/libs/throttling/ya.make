LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/common

    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
