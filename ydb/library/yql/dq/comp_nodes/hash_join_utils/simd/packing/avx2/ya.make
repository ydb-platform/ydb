IF (ARCH_X86_64 AND OS_LINUX)

LIBRARY()

CFLAGS(-mavx2)

SRCS(packing_avx2.cpp)

PEERDIR(
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd
)

END()

ENDIF()
