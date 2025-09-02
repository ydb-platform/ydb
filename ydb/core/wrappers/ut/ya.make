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
        yql/essentials/minikql/comp_nodes/llvm16
    )
    SRCS(
        s3_wrapper_ut.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
