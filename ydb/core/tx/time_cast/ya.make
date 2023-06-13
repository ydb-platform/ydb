LIBRARY()

SRCS(
    time_cast.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tx
)

END()

RECURSE_FOR_TESTS(
    ut
)
