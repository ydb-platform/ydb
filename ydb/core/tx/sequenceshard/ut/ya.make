UNITTEST_FOR(ydb/core/tx/sequenceshard)

PEERDIR(
    ydb/core/testlib/default
    yql/essentials/sql/v1_dummy
)

SRCS(
    ut_helpers.cpp
    ut_sequenceshard.cpp
)

YQL_LAST_ABI_VERSION()

END()
