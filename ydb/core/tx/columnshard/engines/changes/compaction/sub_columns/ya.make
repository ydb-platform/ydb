LIBRARY()

SRCS(
    GLOBAL logic.cpp
    builder.cpp
    remap.cpp
    iterator.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/changes/compaction/common
    ydb/core/formats/arrow/accessor/sub_columns
)

END()
