LIBRARY()

SRCS(
    tracing_support.cpp
)



PEERDIR(
    ydb/core/base
    ydb/library/logger
)

END()

RECURSE_FOR_TESTS(
)
