UNITTEST_FOR(ydb/library/protobuf_printer)

PEERDIR(
    library/cpp/json
    library/cpp/protobuf/json
)

SRCS(
    protobuf_printer_ut.cpp
    security_json_printer_ut.cpp
    test_proto.proto
    test_proto_required.proto
)

END()
