OWNER(g:yql)

LIBRARY()

PEERDIR(
    ydb/library/yql/utils/simd/exec/merge_columns/AVX2_algo
    ydb/library/yql/utils/simd/exec/merge_columns/SSE42_algo
    ydb/library/yql/utils/simd/exec/merge_columns/Fallback_algo
)

END()

RECURSE(
    AVX2_algo
    SSE42_algo
    Fallback_algo
    main
)