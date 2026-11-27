YQL_LIBRARY()

SRCS(
    dq_schedulable.h
    retry_queue.cpp
)

PEERDIR(
    library/cpp/time_provider
    ydb/library/actors/core
    ydb/library/yql/dq/actors/protos
    ydb/library/yverify_stream
    yql/essentials/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)
