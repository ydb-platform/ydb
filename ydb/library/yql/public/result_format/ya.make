LIBRARY()

SRCS(
    yql_result_format.cpp
)

PEERDIR(
    library/cpp/yson/node
    ydb/library/yql/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)

