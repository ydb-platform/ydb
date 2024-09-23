RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/restricted/cityhash-1.0.2

    ydb/library/arrow_clickhouse/Common
    ydb/library/arrow_clickhouse/Columns
    ydb/library/arrow_clickhouse/DataStreams
)

ADDINCL(
    GLOBAL ydb/library/arrow_clickhouse/base
    ydb/library/arrow_clickhouse
)

SRCS(
    AggregateFunctions/IAggregateFunction.cpp
    Aggregator.cpp

    # used in Common/Allocator
    base/common/mremap.cpp
)

END()

RECURSE(
    Columns
    Common
    DataStreams
)
