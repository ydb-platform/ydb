EXECTEST()

RUN(
    stream_store
)

DEPENDS(
    ydb/library/yql/utils/simd/exec/stream_store
)

PEERDIR(
    ydb/library/yql/utils/simd
)

END()

RECURSE(
    stream_store
)