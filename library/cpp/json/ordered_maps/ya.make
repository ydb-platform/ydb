LIBRARY()

PEERDIR(
    library/cpp/json
)

SRCS(
    json_ordered.cpp
    json_value_ordered.cpp
    json_reader_ordered.cpp
    json_writer_ordered.cpp
)

GENERATE_ENUM_SERIALIZATION(json_value_ordered.h)

END()

RECURSE_FOR_TESTS(
    ut
)
