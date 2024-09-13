LIBRARY()

SRCS(
    insert_table.cpp
    rt_insertion.cpp
    user_data.cpp
    inserted.cpp
    committed.cpp
    path_info.cpp
    meta.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/formats/arrow/modifier
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/blobs_action/protos
)

END()
