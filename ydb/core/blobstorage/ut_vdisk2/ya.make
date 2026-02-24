UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    ENV(TIMEOUT=400)
ENDIF()

SRCS(
    defs.h
    env.h
    huge.cpp
    compaction.cpp
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
