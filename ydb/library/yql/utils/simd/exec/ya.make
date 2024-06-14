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

DEPENDS(
    ydb/library/yql/utils/simd/exec/stream_store
    ydb/library/yql/utils/simd/exec/pack_tuple
    ydb/library/yql/utils/simd/exec/tuples_to_bucket
)

PEERDIR(
    ydb/library/yql/utils/simd
)

END()

RECURSE(
    add_columns
    pack_tuple
    stream_store
    tuples_to_bucket
)
