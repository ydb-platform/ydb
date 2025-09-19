IF (ARCH_X86_64 AND OS_LINUX)

PROGRAM()

SRCS(main.cpp)

PEERDIR(ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/runtime_dispatching)

END()

ENDIF()