UNITTEST_FOR(ydb/core/persqueue/pqtablet/quota)

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)
#TIMEOUT(30)

SRCS(
    quota_tracker_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()
