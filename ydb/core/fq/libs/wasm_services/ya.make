LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(metrics.cpp transport.cpp readiness.cpp grpc_readiness.cpp tvm.cpp)

PEERDIR(
    library/cpp/time_provider
    library/cpp/monlib/dynamic_counters
    contrib/libs/curl
    contrib/libs/grpc
    ydb/library/actors/core
    ydb/services/udf_store/wasm
)

END()

RECURSE_FOR_TESTS(ut)


RECURSE(manifest)

RECURSE(query)
