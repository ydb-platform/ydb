UNITTEST()

SIZE(SMALL)

SRCS(
    mdb_endpoint_generator_ut.cpp
)

PEERDIR(
    ydb/core/fq/libs/db_id_async_resolver_impl
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
