LIBRARY()

SRCS(
    yql_langver.cpp
)

PEERDIR(
)

GENERATE_ENUM_SERIALIZATION(yql_langver.h)

END()

RECURSE_FOR_TESTS(
    ut
)
