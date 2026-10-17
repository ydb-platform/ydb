LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
)

ADDINCL(
    ydb/library/arrow_clickhouse/base
    ydb/library/arrow_clickhouse
)

SRCS(
    ColumnsCommon.cpp  # gn: into ydb/library/arrow_clickhouse
    ColumnAggregateFunction.cpp  # gn: into ydb/library/arrow_clickhouse
)

END()
