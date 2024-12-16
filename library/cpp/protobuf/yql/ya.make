LIBRARY()


SRCS(
    descriptor.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/protobuf/dynamic_prototype
    library/cpp/protobuf/json
    library/cpp/protobuf/runtime
    library/cpp/string_utils/base64
)

GENERATE_ENUM_SERIALIZATION(descriptor.h)

END()
