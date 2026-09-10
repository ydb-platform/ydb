UNITTEST()

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)
TIMEOUT(30)

SRCS(
    quota_tracker_ut.cpp
    read_quoter_ut.cpp
    write_quoter_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/persqueue/pqtablet/quota
    ydb/core/testlib/basics
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

END()
