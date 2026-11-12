YQL_LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/types/binary_json
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/utils
    yql/essentials/utils/log
    library/cpp/digest/crc32c
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd
)

IF (ARCH_X86_64 AND OS_LINUX)
    PEERDIR(
        ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/layout/avx2
        ydb/library/yql/dq/comp_nodes/hash_join_utils/simd/layout/sse42
    )
ENDIF()

SRCS(
    tuple.cpp
    accumulator.cpp
    scalar_layout_converter.cpp
    block_layout_converter.cpp
    layout_converter_common.cpp
    page_hash_table.cpp
    alloc.cpp
    spilled_storage.cpp
    join_defs.cpp
    print_unboxed_value.cpp
)

END()

RECURSE(
    simd
)

RECURSE_FOR_TESTS(
    ut
)
