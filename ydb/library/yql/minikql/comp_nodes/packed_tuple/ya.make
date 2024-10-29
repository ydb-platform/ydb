LIBRARY()

SRCS(
    tuple.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/binary_json
    ydb/library/yql/minikql
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    library/cpp/digest/crc32c
)

CFLAGS(
    -mprfchw
    -DMKQL_DISABLE_CODEGEN
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
