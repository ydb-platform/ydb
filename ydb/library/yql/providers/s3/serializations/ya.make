LIBRARY()

SRCS(
    serialization_interval.cpp
)

PEERDIR(
    ydb/library/yql/udfs/common/clickhouse/client
)

ADDINCL(
    ydb/library/yql/udfs/common/clickhouse/client/base
    ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    ydb/library/yql/udfs/common/clickhouse/client/src
)

GENERATE_ENUM_SERIALIZATION(serialization_interval.h)

END()
