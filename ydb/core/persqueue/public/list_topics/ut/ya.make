UNITTEST_FOR(ydb/core/persqueue/public/list_topics)

YQL_LAST_ABI_VERSION()

SRCS(
    list_all_topics_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
)

END()
