UNITTEST_FOR(library/cpp/protobuf/json)

SRCS(
    filter_ut.cpp
    json2proto_ut.cpp
    proto2json_ut.cpp
    inline_ut.proto
    inline_ut.cpp
    string_transform_ut.cpp
    filter_ut.proto
    test.proto
    util_ut.cpp
    unknown_fields_collector_ut.cpp
)

GENERATE_ENUM_SERIALIZATION(test.pb.h)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/protobuf/json
    library/cpp/protobuf/json/proto
)

END()
