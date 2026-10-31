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
<<<<<<< HEAD
=======
    group_state_ut.cpp
    pdisk_filter_compatibility_ut.cpp
>>>>>>> a63405d0931 ([BlobStorage] Promote fail-realm states after durability replication (#57406))
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
