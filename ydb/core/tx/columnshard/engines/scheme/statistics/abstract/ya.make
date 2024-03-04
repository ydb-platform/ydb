LIBRARY()

SRCS(
    portion_storage.cpp
    constructor.cpp
    operator.cpp
    common.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/statistics/protos
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/core/tx/columnshard/splitter
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
