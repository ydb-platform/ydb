LIBRARY()

SRCS(
    dynamic_prototype.cpp
    generate_file_descriptor_set.cpp
)

PEERDIR(
    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(
    ut
)
