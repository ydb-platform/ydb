LIBRARY()

SRCS(
    constructor.cpp
    GLOBAL empty.cpp
    GLOBAL transparent.cpp
    GLOBAL snapshot.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
