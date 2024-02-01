LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/change_exchange
    ydb/core/protos
    ydb/core/scheme
    ydb/core/scheme_types
    ydb/core/tablet_flat
    ydb/core/io_formats/cell_maker
    ydb/core/tx/replication/ydb_proxy
    ydb/library/actors/core
    ydb/library/services
    library/cpp/json
)

SRCS(
    json_change_record.cpp
    service.cpp
    table_writer.cpp
    topic_reader.cpp
    worker.cpp
)

YQL_LAST_ABI_VERSION()

END()
