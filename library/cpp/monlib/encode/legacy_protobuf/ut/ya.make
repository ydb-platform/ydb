UNITTEST_FOR(library/cpp/monlib/encode/legacy_protobuf)

OWNER(
    g:solomon
    msherbakov
)

SRCS(
    legacy_protobuf_ut.cpp
    test_cases.proto
)

PEERDIR(
    library/cpp/monlib/encode/protobuf
    library/cpp/monlib/encode/text
)

END()
