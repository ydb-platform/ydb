LIBRARY()

SRCS(
    constructor.cpp
    GLOBAL zero_level.cpp
    GLOBAL one_layer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
