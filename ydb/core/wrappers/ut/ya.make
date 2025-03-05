UNITTEST_FOR(ydb/core/wrappers)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

IF (NOT OS_WINDOWS)
    PEERDIR(
        ydb/library/actors/core
        library/cpp/digest/md5
        library/cpp/testing/unittest
        ydb/core/protos
        ydb/core/testlib/basics/default
        ydb/core/util
        ydb/core/wrappers/ut_helpers
        ydb/library/yql/minikql/comp_nodes/llvm14
    )
    SRCS(
        s3_wrapper_ut.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:12)

END()
