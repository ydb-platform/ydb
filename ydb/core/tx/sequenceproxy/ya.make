LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/tx/scheme_cache
    ydb/core/tx/sequenceproxy/public
    ydb/core/tx/sequenceshard/public
)

SRCS(
    sequenceproxy.cpp
    sequenceproxy_allocate.cpp
    sequenceproxy_impl.cpp
    sequenceproxy_resolve.cpp
)

END()

RECURSE(
    public
    ut
)

RECURSE_FOR_TESTS(
    ut
)
