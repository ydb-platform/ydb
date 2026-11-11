LIBRARY()

SRCS(
    stream.cpp
    usecases.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/common
    ydb/core/protos
    ydb/core/scheme
)

END()

RECURSE_FOR_TESTS(
    ut
)
