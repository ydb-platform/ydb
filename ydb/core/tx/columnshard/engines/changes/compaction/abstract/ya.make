LIBRARY()

# gn: move into parent

SRCS(
    merger.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/changes/compaction/common
)

END()
