LIBRARY()

SRCS(
    retry_queue.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/public/issue
    ydb/library/yql/dq/actors/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
