LIBRARY()

PEERDIR(
    ydb/library/grpc/server
    ydb/core/base
    ydb/core/client/scheme_cache_lib
    ydb/core/client/server
    ydb/core/engine
    ydb/public/lib/deprecated/kicli
)

END()

RECURSE(
    metadata
    minikql_compile
    minikql_result_lib
    scheme_cache_lib
    server
)

RECURSE_FOR_TESTS(
    ut
)
