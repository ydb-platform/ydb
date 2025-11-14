LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    ydb/core/persqueue/public/mlp
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/public/sdk/cpp/src/client/query
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
