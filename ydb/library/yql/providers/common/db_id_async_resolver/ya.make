LIBRARY()

SRCS(
    db_async_resolver.h
)

PEERDIR(
    library/cpp/threading/future
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
