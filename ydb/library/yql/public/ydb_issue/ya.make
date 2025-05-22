LIBRARY()

SRCS(
    ydb_issue_message.cpp
)

PEERDIR(
    ydb/public/api/protos
    yql/essentials/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)

