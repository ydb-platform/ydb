LIBRARY()

SRCS(
    tuple.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/types/binary_json
    yql/essentials/minikql
    yql/essentials/utils
    yql/essentials/utils/log
    library/cpp/digest/crc32c
)

CFLAGS(
    -mprfchw
    -mavx2
    -DMKQL_DISABLE_CODEGEN
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
