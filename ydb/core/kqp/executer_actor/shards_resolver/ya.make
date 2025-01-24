LIBRARY()

SRCS(
    kqp_shards_resolver.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
)

YQL_LAST_ABI_VERSION()

END()
