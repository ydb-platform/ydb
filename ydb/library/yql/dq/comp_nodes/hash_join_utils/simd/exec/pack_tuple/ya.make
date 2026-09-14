IF (ARCH_X86_64 AND OS_LINUX)

PROGRAM()

SRCS(main.cpp)

CFLAGS(-mavx2)

PEERDIR(ydb/library/yql/dq/comp_nodes/hash_join_utils/simd)

END()

ENDIF()