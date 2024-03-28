EXECTEST()

RUN(
    stream_store
)

RUN(
    pack_tuple
)

RUN(
    tuples_to_bucket
)

RUN(
    merge_columns_benchmark
)

DEPENDS(
    ydb/library/yql/utils/simd/exec/stream_store
    ydb/library/yql/utils/simd/exec/pack_tuple
    ydb/library/yql/utils/simd/exec/tuples_to_bucket
    ydb/library/yql/utils/simd/exec/merge_columns_benchmark
)

PEERDIR(
    ydb/library/yql/utils/simd
)

END()

RECURSE(
    pack_tuple
    tuples_to_bucket
    stream_store
    add_columns
    merge_columns_benchmark
)