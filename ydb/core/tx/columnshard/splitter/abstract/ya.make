LIBRARY()

SRCS(
    chunks.cpp
    chunk_meta.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/abstract
    ydb/core/tx/columnshard/common
)

END()
