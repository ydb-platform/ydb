LIBRARY()

SRCS(
    retry_queue.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
