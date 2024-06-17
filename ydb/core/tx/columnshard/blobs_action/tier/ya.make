LIBRARY()

SRCS(
    adapter.cpp
    gc.cpp
    gc_actor.cpp
    gc_info.cpp
    write.cpp
    read.cpp
    storage.cpp
    remove.cpp
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/core/tablet_flat
    ydb/core/tx/tiering
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/blobs_action/abstract
)

END()
