UNITTEST_FOR(ydb/core/tx/columnshard/blobs_action/tier)

SIZE(SMALL)

SRCS(
    ut_object_key.cpp
)

PEERDIR(
    ydb/core/base
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()