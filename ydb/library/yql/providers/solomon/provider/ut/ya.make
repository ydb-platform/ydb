UNITTEST_FOR(ydb/library/yql/providers/solomon/provider)

SRCS(
    yql_solomon_write_settings_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
