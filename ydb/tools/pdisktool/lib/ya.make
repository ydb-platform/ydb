LIBRARY()

IF (YDB_ENABLE_PDISK_SHRED)
    CFLAGS(
        -DENABLE_PDISK_SHRED
    )
ENDIF()
IF (YDB_DISABLE_PDISK_ENCRYPTION)
    CFLAGS(
        -DDISABLE_PDISK_ENCRYPTION
    )
ENDIF()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/getopt
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/erasure
    ydb/core/protos
    ydb/library/pdisk_io
    ydb/tools/pdisktool/proto
)

SRCS(
    blobs.cpp
    chunk.cpp
    commands.cpp
    device.cpp
    format.cpp
    hull.cpp
    keys.cpp
    log.cpp
    output.cpp
    sector.cpp
    session.cpp
    state.cpp
    syslog.cpp
)

END()
