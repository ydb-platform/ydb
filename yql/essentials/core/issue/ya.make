LIBRARY()

SRCS(
    yql_issue.cpp
)

PEERDIR(
    library/cpp/resource
    contrib/libs/protobuf
    yql/essentials/public/issue
    yql/essentials/core/issue/protos
)

RESOURCE(
    yql/essentials/core/issue/yql_issue.txt yql_issue.txt
)

END()

RECURSE_FOR_TESTS(
    ut
)
