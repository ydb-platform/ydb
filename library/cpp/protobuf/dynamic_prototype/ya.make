LIBRARY()

SRCS(
    dynamic_prototype.cpp
    generate_file_descriptor_set.cpp
)

PEERDIR(
    library/cpp/protobuf/runtime
)

END()

RECURSE_FOR_TESTS(
    ut
)
