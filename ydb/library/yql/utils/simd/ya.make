LIBRARY()

PEERDIR(
    library/cpp/digest/crc32c
    ydb/library/yql/utils/simd/exec/merge_columns/AVX2_algo
    ydb/library/yql/utils/simd/exec/merge_columns/SSE42_algo
    ydb/library/yql/utils/simd/exec/merge_columns/Fallback_algo
    )

END()

RECURSE(
    exec/merge_columns
)

RECURSE_FOR_TESTS(
    ut
    exec
)