LIBRARY()

SRCS(
    reader.cpp
    writer.cpp
)

PEERDIR(
    contrib/libs/libarchive
    library/cpp/json
)

GENERATE_ENUM_SERIALIZATION(reader.h)

END()

