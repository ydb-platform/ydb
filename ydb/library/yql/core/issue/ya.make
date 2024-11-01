LIBRARY()

SRCS(
    yql_issue.cpp
)

PEERDIR(
    library/cpp/resource
    contrib/libs/protobuf
    yql/essentials/public/issue
    ydb/library/yql/core/issue/protos
)

RESOURCE(
    ydb/library/yql/core/issue/yql_issue.txt yql_issue.txt
)

END()

RECURSE_FOR_TESTS(
    ut
)
