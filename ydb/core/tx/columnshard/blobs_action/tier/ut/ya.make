# TODO: Register in RECURSE_FOR_TESTS when the implementation from PR #52993 is available.
UNITTEST_FOR(ydb/core/tx/columnshard/blobs_action/tier)

SIZE(SMALL)

SRCS(
    object_key_ut.cpp
)

PEERDIR(
    ydb/core/base
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
