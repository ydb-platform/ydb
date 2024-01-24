LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/change_exchange
    ydb/core/scheme
    ydb/core/tx/replication/ydb_proxy
    ydb/library/actors/core
)

SRCS(
    service.cpp
    table_writer.cpp
    topic_reader.cpp
    worker.cpp
)

YQL_LAST_ABI_VERSION()

END()
