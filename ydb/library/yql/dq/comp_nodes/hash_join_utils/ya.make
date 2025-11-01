LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/types/binary_json
    yql/essentials/minikql
    yql/essentials/utils
    yql/essentials/utils/log
    library/cpp/digest/crc32c
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd
)

SRCS(
    tuple.cpp
    accumulator.cpp
    block_layout_converter.cpp
    layout_converter_common.cpp
    page_hash_table.cpp
)

IF (ARCH_X86_64 AND OS_LINUX)

CFLAGS(
    -mprfchw
    -mavx2
)

ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    simd
)

RECURSE_FOR_TESTS(
    ut
)
