IF (ARCH_X86_64 AND OS_LINUX)

LIBRARY()

CFLAGS(-msse4.2)

SRCS(packing_sse42.cpp)

PEERDIR(
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd
)

END()

ENDIF()
