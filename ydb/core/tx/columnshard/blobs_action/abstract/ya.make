LIBRARY()

SRCS(
    gc.cpp
    gc_actor.cpp
    common.cpp
    blob_set.cpp
    read.cpp
    write.cpp
    remove.cpp
    storage.cpp
    action.cpp
    storages_manager.cpp
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/core/tablet_flat
    ydb/core/tx/tiering/abstract
    ydb/core/tx/columnshard/blobs_action/common
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/blobs_action/events
    ydb/core/tx/columnshard/blobs_action/protos
)

END()
