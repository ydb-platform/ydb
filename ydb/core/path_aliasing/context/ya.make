LIBRARY()

SRCS(
    path_context.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/path_aliasing
    ydb/library/conclusion
)

END()

RECURSE_FOR_TESTS(
    ut
)
