LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(transport.cpp)

PEERDIR(
    contrib/libs/curl
    contrib/libs/grpc
    ydb/services/udf_store/wasm
)

END()

RECURSE_FOR_TESTS(ut)

RECURSE(query)
