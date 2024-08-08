UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
TIMEOUT(600)

IF (SANITIZER_TYPE)
    ENV(TIMEOUT=400)
ENDIF()

SRCS(
    defs.h
    env.h
    huge.cpp
)

PEERDIR(
    ydb/apps/version
    library/cpp/testing/unittest
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/pdisk/mock
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/tx/scheme_board
    ydb/library/yql/public/udf/service/stub
)

END()
