LIBRARY()

SRCS(
    db_async_resolver_impl.cpp
    mdb_host_transformer.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/core/fq/libs/events
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/dq/actors
)

YQL_LAST_ABI_VERSION()

END()
