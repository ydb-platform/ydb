UNITTEST()

IF (NOT WITH_VALGRIND)
    SRCS(
        main.cpp
    )
ENDIF()

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/apps/version
    ydb/library/actors/interconnect/mock
    library/cpp/testing/unittest
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/pdisk/mock
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/tx/scheme_board
    ydb/core/util
)

END()
