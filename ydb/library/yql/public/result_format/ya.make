LIBRARY()

SRCS(
    yql_result_format.cpp
)

PEERDIR(
    library/cpp/yson/node
)

END()

RECURSE_FOR_TESTS(
    ut
)

