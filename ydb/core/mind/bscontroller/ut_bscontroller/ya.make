UNITTEST()

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/blobstorage/base
    ydb/core/blobstorage/dsproxy/mock
    ydb/core/mind/bscontroller
    ydb/core/protos
    ydb/core/testlib/default
    ydb/core/testlib/basics
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(network:full)

END()
