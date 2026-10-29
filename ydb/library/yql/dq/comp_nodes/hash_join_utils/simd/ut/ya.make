UNITTEST_FOR(ydb/library/yql/dq/comp_nodes/hash_join_utils/simd)

SRCS(
    test_utils_ut.cpp
)

GENERATE_ENUM_SERIALIZATION(test_utils_enums.h)

IF (ARCH_X86_64 AND OS_LINUX)
SRCS(
    simd_ut.cpp
)
ENDIF()

END()
