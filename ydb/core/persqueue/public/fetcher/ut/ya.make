UNITTEST_FOR(ydb/core/persqueue/public/fetcher)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    fetch_request_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/public/sdk/cpp/src/client/query

)

END()
