LIBRARY()

PEERDIR(
    library/cpp/protobuf/json
    library/cpp/json/ordered_maps
)

SRCS(
    json2proto_ordered.cpp
)

END()

RECURSE_FOR_TESTS(
    ../ut
)
