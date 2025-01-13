LIBRARY()

SRCS(
    metadata.cpp
    checksum.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/base
)

GENERATE_ENUM_SERIALIZATION(metadata.h)

END()
