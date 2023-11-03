EXECTEST()

RUN(
    stream_store
)

RUN(
    pack_tuple
)

DEPENDS(
    ydb/library/yql/utils/simd/exec/stream_store
    ydb/library/yql/utils/simd/exec/pack_tuple
)

PEERDIR(
    ydb/library/yql/utils/simd
)

END()

RECURSE(
    pack_tuple
    stream_store
)