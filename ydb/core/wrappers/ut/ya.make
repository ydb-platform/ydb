UNITTEST_FOR(ydb/core/wrappers)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

IF (NOT OS_WINDOWS)
    PEERDIR(
        library/cpp/actors/core
        library/cpp/digest/md5
        library/cpp/testing/unittest
        ydb/core/protos
        ydb/core/testlib/basics/default
        ydb/core/wrappers/ut_helpers
    )
    SRCS(
        s3_wrapper_ut.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:12)

END()
