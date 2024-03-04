LIBRARY()

SRCS(
    granule.cpp
    storage.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/engines/storage/optimizer
    ydb/core/formats/arrow
)

GENERATE_ENUM_SERIALIZATION(granule.h)

END()
