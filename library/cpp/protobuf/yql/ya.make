LIBRARY()

SRCS(
    descriptor.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/json
    library/cpp/protobuf/dynamic_prototype
    library/cpp/protobuf/json
    library/cpp/string_utils/base64
)

END()
