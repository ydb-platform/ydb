LIBRARY()

SRCS(
    abstract.cpp
    snapshot.cpp
    empty.cpp
    transparent.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
