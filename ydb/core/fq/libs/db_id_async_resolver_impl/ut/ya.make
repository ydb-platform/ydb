UNITTEST()

SIZE(SMALL)

SRCS(
    mdb_endpoint_generator_ut.cpp
)

PEERDIR(
    ydb/core/fq/libs/db_id_async_resolver_impl
)

YQL_LAST_ABI_VERSION()

END()
