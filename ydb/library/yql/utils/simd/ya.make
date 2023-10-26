EXECTEST()

RUN(
    stream_store
)

DEPENDS(
    ydb/library/yql/utils/simd/exec/stream_store
)

END()

RECURSE(
    exec
)

RECURSE_FOR_TESTS(
    ut
)