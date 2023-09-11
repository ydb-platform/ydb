LIBRARY()

SRCS(
    abstract.cpp
    bs.cpp
    blob_manager_db.cpp
)

PEERDIR(
    ydb/core/protos
    contrib/libs/apache/arrow
    ydb/core/tablet_flat
    ydb/core/tx/tiering
)

END()
