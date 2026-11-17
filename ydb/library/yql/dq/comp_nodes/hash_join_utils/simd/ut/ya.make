UNITTEST_FOR(ydb/library/yql/dq/comp_nodes/hash_join_utils/simd)

SRCS(
    test_utils_ut.cpp
)

IF (ARCH_X86_64 AND OS_LINUX)
SRCS(
    simd_ut.cpp
)
ENDIF()

END()
