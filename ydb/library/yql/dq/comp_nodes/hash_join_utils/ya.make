LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/types/binary_json
    yql/essentials/minikql
    yql/essentials/utils
    yql/essentials/utils/log
    library/cpp/digest/crc32c
)

IF (ARCH_X86_64 AND OS_LINUX)

PEERDIR(
    ydb/library/yql/dq/comp_nodes/hash_join_utils/simd
)

SRCS(
    tuple.cpp
    accumulator.cpp
    block_layout_converter.cpp
)

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
