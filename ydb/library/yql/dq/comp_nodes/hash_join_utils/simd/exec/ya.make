IF (ARCH_X86_64 AND OS_LINUX)

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
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/stream_store
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/pack_tuple
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/exec/tuples_to_bucket
)

PEERDIR(
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd
)

END()

RECURSE(
    add_columns
    pack_tuple
    runtime_dispatching
    stream_store
    tuples_to_bucket
)

ENDIF()
