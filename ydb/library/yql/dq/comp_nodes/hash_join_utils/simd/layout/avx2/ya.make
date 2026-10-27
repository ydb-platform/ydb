IF (ARCH_X86_64 AND OS_LINUX)

YQL_LIBRARY()

CFLAGS(-mavx2)

SRCS(tuple_avx2.cpp)

PEERDIR(
    yql/essentials/minikql
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd
)

END()

ENDIF()
