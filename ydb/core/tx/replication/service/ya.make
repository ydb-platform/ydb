LIBRARY()

PEERDIR(
    library/cpp/json
    ydb/core/base
    ydb/core/change_exchange
    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/io_formats/cell_maker
    ydb/core/persqueue/purecalc
    ydb/core/protos
    ydb/core/scheme
    ydb/core/scheme_types
    ydb/core/tablet_flat
    ydb/core/tx/replication/common
    ydb/core/tx/replication/ydb_proxy
    ydb/core/wrappers
    ydb/library/actors/core
    ydb/library/services
)

SRCS(
    base_table_writer.cpp
    json_change_record.cpp
    service.cpp
    table_writer.cpp
    topic_reader.cpp
    transfer_writer.cpp
    worker.cpp
)

GENERATE_ENUM_SERIALIZATION(worker.h)

YQL_LAST_ABI_VERSION()

IF (!OS_WINDOWS)
    SRCS(
        s3_writer.cpp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut_json_change_record
    ut_table_writer
    ut_topic_reader
    ut_transfer_writer
    ut_worker
)

IF (!OS_WINDOWS)
    RECURSE_FOR_TESTS(
        ut_s3_writer
    )
ENDIF()
