UNITTEST_FOR(ydb/core/persqueue/public/mlp)

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)
#TIMEOUT(30)

SRCS(
    mlp_changer_ut.cpp
    mlp_reader_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/public/sdk/cpp/src/client/query


)

END()
