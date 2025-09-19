IF (ARCH_X86_64 AND OS_LINUX)

LIBRARY()

PEERDIR(
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/runtime_dispatching/AVX2_algo
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/runtime_dispatching/SSE42_algo
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/runtime_dispatching/Fallback_algo
)

END()

RECURSE(
    AVX2_algo
    Fallback_algo
    main
    SSE42_algo
)

ENDIF()
