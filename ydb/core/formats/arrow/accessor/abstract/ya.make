LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/conclusion
    ydb/services/metadata/abstract
    ydb/library/formats/arrow/accessor/abstract
    ydb/library/formats/arrow/accessor/common
    ydb/library/formats/arrow/protos
    ydb/library/actors/core
)

SRCS(
    constructor.cpp
    request.cpp
)

END()
