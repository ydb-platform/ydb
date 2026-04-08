UNITTEST_FOR(ydb/library/protobuf_printer)

PEERDIR(
    library/cpp/protobuf/json
)

SRCS(
    protobuf_printer_ut.cpp
    test_proto.proto
)

END()
