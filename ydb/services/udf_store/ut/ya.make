UNITTEST()

PEERDIR(
    library/cpp/json
    library/cpp/testing/unittest
)

SRCS(
    manifest_ut.cpp
    ../wasm/manifest.cpp
)

END()
