LIBRARY()

SRCS(
    iscan.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)