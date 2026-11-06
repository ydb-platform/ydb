YQL_LIBRARY()

SRCS(
    table_checksum.cpp
)

PEERDIR(
    ydb/core/backup/common
    ydb/library/query_actor
)

END()
