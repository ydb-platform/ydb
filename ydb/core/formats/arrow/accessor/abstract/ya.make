LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/services/metadata/abstract
    ydb/library/actors/core
    ydb/core/formats/arrow/accessor/common
    ydb/library/formats/arrow/protos
)

SRCS(
    constructor.cpp
    request.cpp
    accessor.cpp
)

GENERATE_ENUM_SERIALIZATION(accessor.h)

END()
