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

IF (ARCH_X86_64)

SRCS(
    tuple.cpp
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
