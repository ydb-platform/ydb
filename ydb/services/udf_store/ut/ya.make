UNITTEST()

PEERDIR(
    library/cpp/json
    library/cpp/testing/unittest
    ydb/core/protos
)

SRCS(
    manifest_ut.cpp
    blob_chunks_ut.cpp
    ../wasm/manifest.cpp
    ../blob_chunks.cpp
)

END()
