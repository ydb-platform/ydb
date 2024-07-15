LIBRARY()

SRCS(
    portion_info.cpp
    column_record.cpp
    with_blobs.cpp
    meta.cpp
    common.cpp
    index_chunk.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/tx/columnshard/splitter
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tablet_flat
)

GENERATE_ENUM_SERIALIZATION(portion_info.h)

END()
