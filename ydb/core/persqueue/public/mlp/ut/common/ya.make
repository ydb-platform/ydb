LIBRARY()

SRCS(
    common.h
)

PEERDIR(
    ydb/core/persqueue/public/mlp
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/public/sdk/cpp/src/client/query
    library/cpp/testing/unittest
)

END()
