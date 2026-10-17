YQL_LIBRARY()

SRCS(
    retry_queue.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/dq/actors/protos
    ydb/library/yverify_stream
    yql/essentials/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)
