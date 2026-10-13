LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(transport.cpp tvm.cpp)

PEERDIR(
    contrib/libs/curl
    contrib/libs/grpc
    ydb/library/actors/core
    ydb/services/udf_store/wasm
)

END()

RECURSE_FOR_TESTS(ut)

RECURSE(query)
