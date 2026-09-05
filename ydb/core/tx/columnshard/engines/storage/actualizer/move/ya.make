LIBRARY()

SRCS(
    move.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/actualizer/abstract
    ydb/core/tx/columnshard/engines/storage/actualizer/common
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/engines/changes/actualization/construction
    ydb/core/tx/columnshard/engines/scheme/versions
)

END()
