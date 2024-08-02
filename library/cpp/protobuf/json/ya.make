LIBRARY()

SRCS(
    json2proto.cpp
    json_output_create.cpp
    json_value_output.cpp
    json_writer_output.cpp
    name_generator.cpp
    proto2json.cpp
    proto2json_printer.cpp
    string_transform.cpp
    util.h
    util.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/protobuf/runtime
    library/cpp/protobuf/util
    library/cpp/protobuf/json/proto
    library/cpp/string_utils/relaxed_escaper
)

END()

RECURSE_FOR_TESTS(
    ut
)
