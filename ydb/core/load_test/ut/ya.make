UNITTEST_FOR(ydb/core/load_test)

FORK_SUBTESTS(MODULO)
SPLIT_FACTOR(11)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ENDIF()

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    group_test_ut.cpp
    nbs_dbg_like_alloc_helper_ut.cpp
    util_ut.cpp
)

END()
