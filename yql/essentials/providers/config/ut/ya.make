UNITTEST_FOR(yql/essentials/providers/config)

SRCS(
    yql_config_provider_statistics_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
