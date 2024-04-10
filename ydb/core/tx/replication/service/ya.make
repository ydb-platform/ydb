LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/change_exchange
    ydb/core/protos
    ydb/core/scheme
    ydb/core/scheme_types
    ydb/core/tablet_flat
    ydb/core/io_formats/cell_maker
    ydb/core/tx/replication/common
    ydb/core/tx/replication/ydb_proxy
    ydb/library/actors/core
    ydb/library/services
    ydb/core/wrappers
    library/cpp/json
)

SRCS(
    json_change_record.cpp
    service.cpp
    table_writer.cpp
    s3_writer.cpp
    topic_reader.cpp
    worker.cpp
)

GENERATE_ENUM_SERIALIZATION(worker.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut_s3_writer
    ut_table_writer
    ut_topic_reader
    ut_worker
)
