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
    library/cpp/blockcodecs
    library/cpp/getopt
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/erasure
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/library/pdisk_io
    ydb/tools/pdisktool/proto
    # tablet_flat can hold Postgres-typed columns; the tool never has to interpret them, so the stub
    # implementation of the type registry is enough and saves linking the parser.
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    blobs.cpp
    blobsource.cpp
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
    tabletdb.cpp
    tabletdump.cpp
    tabletlog.cpp
)

END()
