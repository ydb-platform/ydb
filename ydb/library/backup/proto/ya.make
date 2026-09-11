LIBRARY()

SRCS(
    proto.cpp
)

PEERDIR(
    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(
    ut
)
