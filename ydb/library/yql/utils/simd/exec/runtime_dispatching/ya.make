OWNER(g:yql)

LIBRARY()

PEERDIR(
    ydb/library/yql/utils/simd/exec/runtime_dispatching/AVX2_algo
    ydb/library/yql/utils/simd/exec/runtime_dispatching/SSE42_algo
    ydb/library/yql/utils/simd/exec/runtime_dispatching/Fallback_algo
)

END()

RECURSE(
    AVX2_algo
    SSE42_algo
    Fallback_algo
    main
)

RECURSE_FOR_TESTS(
    ut
)