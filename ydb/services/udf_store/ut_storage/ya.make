UNITTEST()

SIZE(MEDIUM)
FORK_SUBTESTS()
YQL_LAST_ABI_VERSION()

SRCS(
    artifact_state_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/kqp/ut/common
    ydb/public/sdk/cpp/src/client/proto
    ydb/services/udf_store
    yql/essentials/sql/pg_dummy
)

END()
