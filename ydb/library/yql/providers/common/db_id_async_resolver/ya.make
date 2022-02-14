OWNER(g:yq)

LIBRARY()

SRCS(
    db_async_resolver_impl.cpp
    db_async_resolver_with_meta.cpp
)

PEERDIR(
    ydb/core/yq/libs/common
    ydb/core/yq/libs/events
)

YQL_LAST_ABI_VERSION()

END()
