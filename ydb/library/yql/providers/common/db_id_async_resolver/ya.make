OWNER(g:yq)

LIBRARY()

SRCS(
    db_async_resolver_impl.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/core/yq/libs/events
    ydb/library/yql/providers/dq/actors
)

YQL_LAST_ABI_VERSION()

END()
