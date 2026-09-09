LIBRARY()

SRCS(
    address.cpp
    gc.cpp
    gc_actor.cpp
    history_cutter.cpp
    write.cpp
    read.cpp
    storage.cpp
    remove.cpp
    blob_manager.cpp
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/core/tablet_flat
    ydb/core/tx/tiering
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/blobs_action/abstract
)

END()
