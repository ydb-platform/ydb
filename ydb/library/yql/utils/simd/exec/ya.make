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
    pack_tuple
    tuples_to_bucket
    stream_store
)