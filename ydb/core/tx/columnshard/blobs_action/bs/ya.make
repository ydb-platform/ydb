LIBRARY()

SRCS(
    gc.cpp
    gc_actor.cpp
    write.cpp
    read.cpp
    storage.cpp
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/core/tablet_flat
    ydb/core/tx/tiering
)

END()
