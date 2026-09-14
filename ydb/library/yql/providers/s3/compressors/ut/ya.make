IF (NOT OS_WINDOWS AND CLANG AND NOT WITH_VALGRIND)

UNITTEST_FOR(ydb/library/yql/providers/s3/compressors)

SRCS(
    decompressor_ut.cpp
    output_queue_ut.cpp
)

PEERDIR(
    library/cpp/scheme
    ydb/library/yql/udfs/common/clickhouse/client
    yql/essentials/public/udf/service/stub
)

ADDINCL(
    ydb/library/yql/udfs/common/clickhouse/client/base
    ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    ydb/library/yql/udfs/common/clickhouse/client/src
)

END()

ENDIF()

