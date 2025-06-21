LIBRARY()


SRCS(
    meta.cpp
    constructor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/abstract
    ydb/core/tx/columnshard/engines/scheme/indexes/abstract
)

END()
