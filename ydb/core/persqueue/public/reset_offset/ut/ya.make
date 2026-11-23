UNITTEST_FOR(ydb/core/persqueue/public/reset_offset)

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)

SRCS(
    reset_offset_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/persqueue/public/describer
    ydb/core/persqueue/public/reset_offset
    ydb/core/testlib
    ydb/core/testlib/actors
    ydb/library/aclib
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    library/cpp/testing/unittest
)

ENV(INSIDE_YDB="1")

END()

RECURSE(
    sim
)
