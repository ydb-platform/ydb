UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
    ENV(TIMEOUT=400)
ELSE()
    REQUIREMENTS(ram:16 cpu:1)
ENDIF()

SRCS(
    defs.h
    env.h
    blob_stat.cpp
    huge.cpp
    compaction.cpp
    space_report.cpp
)

PEERDIR(
    contrib/libs/xxhash
    ydb/apps/version
    library/cpp/testing/unittest
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/pdisk/mock
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/tx/scheme_board
    yql/essentials/public/udf/service/stub
    ydb/core/util/actorsys_test
)

END()
