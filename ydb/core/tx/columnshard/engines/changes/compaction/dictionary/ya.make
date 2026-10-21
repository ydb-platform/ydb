LIBRARY()

SRCS(
    GLOBAL logic.cpp
)

PEERDIR(
    ydb/core/formats/arrow/filter
    ydb/core/tx/columnshard/engines/changes/compaction/common
)

END()
