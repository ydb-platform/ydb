LIBRARY()

PEERDIR(
    ydb/core/formats/arrow/protos
    ydb/core/formats/arrow/accessor/common
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/services/metadata/abstract
)

SRCS(
    accessor.cpp
    constructor.cpp
    request.cpp
)

END()
