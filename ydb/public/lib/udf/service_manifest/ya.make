LIBRARY()

SRCS(manifest.cpp auth.cpp)

PEERDIR(
    library/cpp/json
    ydb/public/lib/udf/manifest
    ydb/udfs/wasm/sdk/services
)

END()
