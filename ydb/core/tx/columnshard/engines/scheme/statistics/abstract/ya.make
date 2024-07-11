LIBRARY()

SRCS(
    portion_storage.cpp
    constructor.cpp
    operator.cpp
    common.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/statistics/protos
    ydb/core/tx/columnshard/engines/scheme/abstract
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/library/conclusion
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
