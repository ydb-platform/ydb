LIBRARY()

SRCS(
    gc.cpp
    common.cpp
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
    ydb/core/tx/tiering
)

END()
