LIBRARY()

SRCS(
    granule.cpp
    storage.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
)

GENERATE_ENUM_SERIALIZATION(granule.h)

END()
