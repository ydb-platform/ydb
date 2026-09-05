LIBRARY()

SRCS(
    index.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/versions
    ydb/core/tx/columnshard/engines/storage/actualizer/move
)

END()
