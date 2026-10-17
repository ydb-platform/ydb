LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
)

ADDINCL(
    ydb/library/arrow_clickhouse/base
    ydb/library/arrow_clickhouse
)

SRCS(
    AggregatingBlockInputStream.cpp  # gn: into ydb/library/arrow_clickhouse
    IBlockInputStream.cpp  # gn: into ydb/library/arrow_clickhouse
    MergingAggregatedBlockInputStream.cpp  # gn: into ydb/library/arrow_clickhouse
)

END()
