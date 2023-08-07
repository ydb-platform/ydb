LIBRARY()

SRCS(
    db_async_resolver.h
    mdb_host_transformer.h
)

PEERDIR(
    library/cpp/threading/future
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
