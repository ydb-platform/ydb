LIBRARY()

# gn: move into parent

SRCS(
    GLOBAL logic.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/changes/compaction/common
)

END()
