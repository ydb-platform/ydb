UNITTEST_FOR(ydb/core/fq/libs/row_dispatcher)

SRCS(
    leader_detector_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/actors
    ydb/core/testlib/basics/default
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
