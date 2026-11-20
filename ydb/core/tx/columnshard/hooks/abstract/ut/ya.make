UNITTEST_FOR(ydb/core/tx/columnshard/hooks/abstract)

SIZE(SMALL)

SRCS(
    controllers_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
