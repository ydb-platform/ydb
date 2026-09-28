UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/blobstorage/pdisk
    ydb/core/erasure
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/core/testlib/actors
    ydb/library/pdisk_io
    ydb/tools/pdisktool/lib
)

YQL_LAST_ABI_VERSION()

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

SRCS(
    pdisktool_version_ut.cpp
    pdisktool_oracle_ut.cpp
    pdisktool_tablet_ut.cpp
)

END()
