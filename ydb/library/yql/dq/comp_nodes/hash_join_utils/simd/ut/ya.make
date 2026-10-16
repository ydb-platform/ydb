IF (ARCH_X86_64 AND OS_LINUX)

UNITTEST_FOR(ydb/library/yql/dq/comp_nodes/hash_join_utils/simd)

REQUIREMENTS(cpu:1)
SRCS(
    simd_ut.cpp
)

END()

ENDIF()