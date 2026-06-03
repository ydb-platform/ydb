LIBRARY()

SRCS(
    GLOBAL logic.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/changes/compaction/common
    ydb/core/formats/arrow/accessor/compact_kv
)

END()
