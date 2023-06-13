LIBRARY()

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/utils
    ydb/library/yql/public/udf
    ydb/core/formats
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

SRCS(
    sharding.cpp
    hash.cpp
    xx_hash.cpp
    unboxed_reader.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)