LIBRARY()

SRCS(
    GLOBAL logic.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/changes/compaction/common
)

END()
