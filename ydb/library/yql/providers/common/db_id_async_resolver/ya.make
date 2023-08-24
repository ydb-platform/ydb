LIBRARY()

SRCS(
    db_async_resolver.h
    mdb_endpoint_generator.h
)

PEERDIR(
    library/cpp/threading/future
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
