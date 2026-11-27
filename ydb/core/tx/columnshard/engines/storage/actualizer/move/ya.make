LIBRARY()

SRCS(
    move.cpp
    portion_queue.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/tx/columnshard/engines/storage/actualizer/abstract
    ydb/core/tx/columnshard/engines/storage/actualizer/common
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/engines/changes/actualization/construction
    ydb/core/tx/columnshard/engines/scheme/versions
)

END()
