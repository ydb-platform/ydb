UNITTEST_FOR(ydb/core/persqueue/public/set_offsets)

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)

SRCS(
    set_offsets_sim_ut.cpp
)

PEERDIR(
    library/cpp/containers/absl
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/persqueue/public/describer
    ydb/core/persqueue/public/set_offsets
    ydb/core/testlib
    ydb/core/testlib/actors
    ydb/library/aclib
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    library/cpp/testing/unittest
    library/cpp/threading/future
)

ENV(INSIDE_YDB="1")

END()
